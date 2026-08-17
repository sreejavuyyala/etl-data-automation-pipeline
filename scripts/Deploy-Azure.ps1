#Requires -Version 7.4
<#
.SYNOPSIS
    Deploys the Sales ETL pipeline to Azure: infrastructure, secrets, and ADF artifacts.

.DESCRIPTION
    Runs the four steps that stand the pipeline up in a subscription:

      1. Deploy infra/main.bicep  -- SQL server, two databases, Key Vault, data factory
      2. Store connection strings and the alert webhook URL in Key Vault
      3. Publish the ADF linked services, datasets, pipelines and trigger from adf/
      4. Deploy the schema in sql/ into the target database

    Every secret is written to Key Vault and read from there at runtime by the
    factory's managed identity. Nothing in this repository holds a credential,
    and this script never writes one to disk.

    -------------------------------------------------------------------------
    NOT YET EXECUTED. This script has not been run against a live Azure
    subscription -- see the note in infra/main.bicep. It is written against the
    az CLI 2.58-2.60 surface but should be treated as reviewed, not verified.
    Run it with -WhatIf first.
    -------------------------------------------------------------------------

.PARAMETER ResourceGroup
    Resource group to deploy into. Created if absent.

.PARAMETER Location
    Azure region. Defaults to eastus.

.PARAMETER SqlAdminLogin
    Administrator login for the Azure SQL logical server.

.PARAMETER SqlAdminPassword
    Administrator password, as a SecureString. Prompted for if omitted.

.PARAMETER SkipInfrastructure
    Skip the Bicep deployment and only publish artifacts to existing resources.

.EXAMPLE
    ./scripts/Deploy-Azure.ps1 -ResourceGroup rg-etl-salesdw -SqlAdminLogin etladmin

.EXAMPLE
    ./scripts/Deploy-Azure.ps1 -ResourceGroup rg-etl-salesdw -SqlAdminLogin etladmin -WhatIf
#>

[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)]
    [string] $ResourceGroup,

    [string] $Location = 'eastus',

    [Parameter(Mandatory = $true)]
    [string] $SqlAdminLogin,

    [securestring] $SqlAdminPassword,

    [switch] $SkipInfrastructure
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $PSScriptRoot

function Write-Step { param([string] $Message) Write-Host "==> $Message" -ForegroundColor Cyan }
function Write-Ok   { param([string] $Message) Write-Host "  ok $Message" -ForegroundColor Green }

# -----------------------------------------------------------------------------
#  Preflight
# -----------------------------------------------------------------------------
Write-Step 'Checking prerequisites'

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw 'Azure CLI not found. Install it: https://learn.microsoft.com/cli/azure/install-azure-cli'
}

$azVersion = (az version --output json | ConvertFrom-Json).'azure-cli'
Write-Ok "Azure CLI $azVersion"

$account = az account show --output json 2>$null | ConvertFrom-Json
if (-not $account) { throw 'Not signed in. Run: az login' }
Write-Ok "Subscription: $($account.name)"

if (-not $SqlAdminPassword) {
    $SqlAdminPassword = Read-Host -Prompt 'SQL administrator password' -AsSecureString
}
# Converted only at the point of use, and never written to a file or logged.
$plainPassword = [System.Net.NetworkCredential]::new('', $SqlAdminPassword).Password

# -----------------------------------------------------------------------------
#  1. Infrastructure
# -----------------------------------------------------------------------------
if (-not $SkipInfrastructure) {
    Write-Step "Ensuring resource group $ResourceGroup"
    if ($PSCmdlet.ShouldProcess($ResourceGroup, 'Create resource group')) {
        az group create --name $ResourceGroup --location $Location --output none
        Write-Ok 'Resource group ready'
    }

    Write-Step 'Deploying infra/main.bicep'
    $adminObjectId = az ad signed-in-user show --query id --output tsv 2>$null

    if ($PSCmdlet.ShouldProcess($ResourceGroup, 'Deploy Bicep template')) {
        $deployment = az deployment group create `
            --resource-group $ResourceGroup `
            --template-file (Join-Path $RepoRoot 'infra/main.bicep') `
            --parameters sqlAdminLogin=$SqlAdminLogin `
                         sqlAdminPassword=$plainPassword `
                         adminObjectId=$adminObjectId `
            --output json | ConvertFrom-Json

        $outputs = $deployment.properties.outputs
        Write-Ok "SQL server:   $($outputs.sqlServerFqdn.value)"
        Write-Ok "Key Vault:    $($outputs.keyVaultName.value)"
        Write-Ok "Data factory: $($outputs.dataFactoryName.value)"
    }
}

# Read the deployed names back, so -SkipInfrastructure works against an
# existing deployment without the caller having to repeat them.
Write-Step 'Resolving deployed resource names'
$sqlServerFqdn   = az sql server list --resource-group $ResourceGroup --query '[0].fullyQualifiedDomainName' --output tsv
$keyVaultName    = az keyvault list   --resource-group $ResourceGroup --query '[0].name' --output tsv
$dataFactoryName = az datafactory list --resource-group $ResourceGroup --query '[0].name' --output tsv

if (-not $sqlServerFqdn)   { throw "No SQL server found in $ResourceGroup." }
if (-not $keyVaultName)    { throw "No Key Vault found in $ResourceGroup." }
if (-not $dataFactoryName) { throw "No data factory found in $ResourceGroup. Install the extension: az extension add --name datafactory" }

Write-Ok "SQL $sqlServerFqdn / KV $keyVaultName / ADF $dataFactoryName"

# -----------------------------------------------------------------------------
#  2. Secrets
#
#  The connection strings are assembled here and pushed straight to Key Vault.
#  They are never written to a file, echoed, or passed on a command line that
#  would land in shell history.
# -----------------------------------------------------------------------------
Write-Step 'Storing secrets in Key Vault'

$secrets = @{
    'adventureworks-source-connectionstring' =
        "Server=tcp:$sqlServerFqdn,1433;Initial Catalog=AdventureWorks2022;User ID=$SqlAdminLogin;Password=$plainPassword;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
    'salesreportingdw-target-connectionstring' =
        "Server=tcp:$sqlServerFqdn,1433;Initial Catalog=SalesReportingDW;User ID=$SqlAdminLogin;Password=$plainPassword;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
}

foreach ($name in $secrets.Keys) {
    if ($PSCmdlet.ShouldProcess($name, 'Set Key Vault secret')) {
        az keyvault secret set --vault-name $keyVaultName --name $name --value $secrets[$name] --output none
        Write-Ok "Secret $name"
    }
}

Write-Host '  note The alert webhook secret (etl-alert-logicapp-url) is set separately,' -ForegroundColor Yellow
Write-Host '       after deploying adf/alerts/LogicApp_EtlFailureNotification.json:' -ForegroundColor Yellow
Write-Host '         az keyvault secret set --vault-name ' -NoNewline -ForegroundColor Yellow
Write-Host "$keyVaultName --name etl-alert-logicapp-url --value '<callback url>'" -ForegroundColor Yellow

# -----------------------------------------------------------------------------
#  3. ADF artifacts
#
#  Ordering matters and is not incidental: a dataset cannot be created before
#  the linked service it references, and a pipeline cannot be created before its
#  datasets or before the child pipelines it calls.
# -----------------------------------------------------------------------------
Write-Step 'Publishing Data Factory artifacts'

$publishOrder = @(
    @{ Kind = 'linked-service'; Path = 'adf/linkedService'; Command = 'linked-service' }
    @{ Kind = 'dataset';        Path = 'adf/dataset';       Command = 'dataset' }
    @{ Kind = 'pipeline';       Path = 'adf/pipeline';      Command = 'pipeline' }
    @{ Kind = 'trigger';        Path = 'adf/trigger';       Command = 'trigger' }
)

# Child pipelines must exist before the orchestrator that references them.
$pipelinePriority = @(
    'LoadSalesData_Extract_Pipeline',
    'ValidateSalesData_Pipeline',
    'RaiseAlert_Pipeline',
    'LoadSalesData_Pipeline'
)

foreach ($group in $publishOrder) {
    $directory = Join-Path $RepoRoot $group.Path
    if (-not (Test-Path $directory)) { continue }

    $files = Get-ChildItem -Path $directory -Filter '*.json'
    if ($group.Kind -eq 'pipeline') {
        $files = $files | Sort-Object { $pipelinePriority.IndexOf($_.BaseName) }
    }

    foreach ($file in $files) {
        $name = $file.BaseName
        if ($PSCmdlet.ShouldProcess($name, "Create ADF $($group.Kind)")) {
            az datafactory $group.Command create `
                --resource-group $ResourceGroup `
                --factory-name $dataFactoryName `
                --name $name `
                --properties "@$($file.FullName)" `
                --output none
            Write-Ok "$($group.Kind): $name"
        }
    }
}

# -----------------------------------------------------------------------------
#  4. Database schema
# -----------------------------------------------------------------------------
Write-Step 'Deploying database schema'

$schemaScripts = @(
    'sql/02_target_schema.sql',
    'sql/03_etl_procedures.sql',
    'sql/04_validation_checks.sql'
)

foreach ($script in $schemaScripts) {
    $path = Join-Path $RepoRoot $script
    if ($PSCmdlet.ShouldProcess($script, 'Run against SalesReportingDW')) {
        # sqlcmd rather than `az sql db execute`, which does not exist. Requires
        # mssql-tools18 on the deploying machine.
        & sqlcmd -S $sqlServerFqdn -d 'SalesReportingDW' -U $SqlAdminLogin -P $plainPassword -b -i $path
        if ($LASTEXITCODE -ne 0) { throw "Failed to apply $script" }
        Write-Ok $script
    }
}

# -----------------------------------------------------------------------------
Write-Host ''
Write-Host 'Deployment complete.' -ForegroundColor Green
Write-Host ''
Write-Host 'Remaining manual steps:' -ForegroundColor Cyan
Write-Host '  1. Restore AdventureWorks2022 into the source database (docs/runbook.md).'
Write-Host '  2. Deploy adf/alerts/LogicApp_EtlFailureNotification.json and store its'
Write-Host '     callback URL in Key Vault as etl-alert-logicapp-url.'
Write-Host '  3. Deploy adf/alerts/AzureMonitor_PipelineFailedAlert.json with the'
Write-Host '     action group id that template outputs.'
Write-Host "  4. Start the trigger once you have verified a manual run:"
Write-Host "       az datafactory trigger start --resource-group $ResourceGroup --factory-name $dataFactoryName --name TR_LoadSalesData_Daily_0200"
Write-Host ''
Write-Host '  The trigger ships with runtimeState=Stopped on purpose -- a scheduled' -ForegroundColor Yellow
Write-Host '  pipeline should never start firing as a side effect of a deployment.' -ForegroundColor Yellow
