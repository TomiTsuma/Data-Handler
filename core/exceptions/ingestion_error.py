class IngestionError(Exception):
    """Raised when an ingestion pipeline fails."""


class KaggleDownloadError(IngestionError):
    """Raised when the Kaggle API returns an error."""
