"""Local runner for the Sales ETL pipeline.

The production orchestrator is Azure Data Factory (see ``adf/``). This package
runs the identical stages against the identical stored procedures from a shell,
which is what makes the pipeline testable, and what produced the measurements
reported in the README.
"""

from .config import Settings
from .pipeline import RunResult, SalesDataPipeline

__all__ = ["Settings", "SalesDataPipeline", "RunResult"]
__version__ = "1.0.0"
