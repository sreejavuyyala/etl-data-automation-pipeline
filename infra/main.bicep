// =============================================================================
//  Azure infrastructure for the Sales ETL pipeline.
//
//  Provisions the target Azure SQL Database, the data factory, the Key Vault
//  that holds every secret, and the role assignments that let them talk to one
//  another without a password appearing anywhere.
//
//      az deployment group create \
//        --resource-group rg-etl-salesdw \
//        --template-file infra/main.bicep \
//        --parameters sqlAdminLogin=<login> sqlAdminPassword=<password>
//
//  ---------------------------------------------------------------------------
//  DEPLOYMENT STATUS: this template has NOT been deployed to a live Azure
//  subscription. It is written against the documented resource schemas and the
//  API versions current in 2024, and it is what the deployment would use -- but
//  no `az deployment group create` has ever been run against it, so treat it as
//  reviewed-not-executed. Everything in reports/metrics.md was measured against
//  a local SQL Server 2022 instance, not against resources this file created.
//  See docs/metrics-methodology.md.
//  ---------------------------------------------------------------------------
// =============================================================================

targetScope = 'resourceGroup'

@description('Short name used to derive every resource name.')
param projectName string = 'etl-salesdw'

@description('Location for all resources.')
param location string = resourceGroup().location

@description('Administrator login for the Azure SQL logical server.')
param sqlAdminLogin string

@description('Administrator password. Pass via --parameters or a Key Vault reference; never commit it.')
@secure()
param sqlAdminPassword string

@description('Object id of the Entra ID user or group to grant Key Vault secret access, for setup.')
param adminObjectId string = ''

@description('Azure SQL Database SKU. Basic is enough for AdventureWorks-sized data and is the cheapest option that is not serverless.')
@allowed([
  'Basic'
  'S0'
  'S1'
  'GP_S_Gen5_1'
])
param sqlSkuName string = 'S0'

// -----------------------------------------------------------------------------
//  Names. uniqueString keeps the globally-scoped names (Key Vault, SQL server)
//  collision-free without anyone having to invent one.
// -----------------------------------------------------------------------------
var suffix = uniqueString(resourceGroup().id)
var sqlServerName = 'sql-${projectName}-${suffix}'
var keyVaultName = take('kv-${projectName}-${suffix}', 24)
var dataFactoryName = 'adf-${projectName}'
var targetDatabaseName = 'SalesReportingDW'
var sourceDatabaseName = 'AdventureWorks2022'

// Built-in role: Key Vault Secrets User. Read secret values, nothing else.
var keyVaultSecretsUserRoleId = '4633458b-17de-408a-b874-0445c86b69e6'
// Built-in role: Key Vault Secrets Officer. Create and update secrets, for setup.
var keyVaultSecretsOfficerRoleId = 'b86a8fe4-44ce-4948-aee5-eccb2c155cd7'

// =============================================================================
//  SQL
// =============================================================================
resource sqlServer 'Microsoft.Sql/servers@2023-05-01-preview' = {
  name: sqlServerName
  location: location
  properties: {
    administratorLogin: sqlAdminLogin
    administratorLoginPassword: sqlAdminPassword
    version: '12.0'
    // Refuse unencrypted connections outright rather than relying on every
    // client to ask for TLS.
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

// Allows Azure services -- the data factory's AutoResolve Integration Runtime
// among them -- to reach the server. The 0.0.0.0 sentinel is Azure's documented
// convention for this and does not open the server to the public internet.
resource allowAzureServices 'Microsoft.Sql/servers/firewallRules@2023-05-01-preview' = {
  parent: sqlServer
  name: 'AllowAllWindowsAzureIps'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

@description('Staging and reporting database. Holds the stg, dw and etl schemas.')
resource targetDatabase 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  parent: sqlServer
  name: targetDatabaseName
  location: location
  sku: {
    name: sqlSkuName
    tier: sqlSkuName == 'Basic' ? 'Basic' : (startsWith(sqlSkuName, 'GP_S') ? 'GeneralPurpose' : 'Standard')
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    // Local redundancy: this database is rebuildable from the OLTP source, so
    // paying for geo-redundant backup storage would buy nothing.
    requestedBackupStorageRedundancy: 'Local'
  }
}

@description('Source OLTP database. Restore AdventureWorks2022 into this -- see docs/runbook.md.')
resource sourceDatabase 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  parent: sqlServer
  name: sourceDatabaseName
  location: location
  sku: {
    name: sqlSkuName
    tier: sqlSkuName == 'Basic' ? 'Basic' : (startsWith(sqlSkuName, 'GP_S') ? 'GeneralPurpose' : 'Standard')
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    requestedBackupStorageRedundancy: 'Local'
  }
}

// =============================================================================
//  Key Vault
//
//  RBAC rather than access policies: access policies are the older model and
//  cannot express "this managed identity may read secrets and do nothing else"
//  without also granting it to every other principal on the same policy.
// =============================================================================
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    tenantId: subscription().tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
    // Left off deliberately: purge protection cannot be disabled once enabled,
    // and a portfolio resource group needs to be deletable.
    enablePurgeProtection: null
    publicNetworkAccess: 'Enabled'
  }
}

// =============================================================================
//  Data Factory
// =============================================================================
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  identity: {
    // System-assigned: the factory's identity lives and dies with the factory,
    // so there is no orphaned principal left behind if the resource group goes.
    type: 'SystemAssigned'
  }
  properties: {}
}

// -----------------------------------------------------------------------------
//  Role assignments.
//
//  This is the part that removes credentials from the repository: the factory
//  authenticates to Key Vault as itself, reads the connection strings at
//  runtime, and nothing in git ever holds a secret.
// -----------------------------------------------------------------------------
resource factoryReadsSecrets 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: keyVault
  name: guid(keyVault.id, dataFactory.id, keyVaultSecretsUserRoleId)
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', keyVaultSecretsUserRoleId)
    principalId: dataFactory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource adminManagesSecrets 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(adminObjectId)) {
  scope: keyVault
  name: guid(keyVault.id, adminObjectId, keyVaultSecretsOfficerRoleId)
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', keyVaultSecretsOfficerRoleId)
    principalId: adminObjectId
    principalType: 'User'
  }
}

// =============================================================================
//  Outputs
// =============================================================================
output sqlServerFqdn string = sqlServer.properties.fullyQualifiedDomainName
output keyVaultName string = keyVault.name
output dataFactoryName string = dataFactory.name
output dataFactoryPrincipalId string = dataFactory.identity.principalId
output targetDatabaseName string = targetDatabase.name
output sourceDatabaseName string = sourceDatabase.name

@description('Secrets that must exist in Key Vault before the factory can run. Populate them with scripts/Set-KeyVaultSecrets.ps1.')
output requiredSecretNames array = [
  'adventureworks-source-connectionstring'
  'salesreportingdw-target-connectionstring'
  'etl-alert-logicapp-url'
]
