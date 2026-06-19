class StorageError(Exception):
    """Base storage exception."""


class BucketCreationError(StorageError):
    """Raised when a MinIO bucket cannot be created."""


class ObjectUploadError(StorageError):
    """Raised when an object upload fails."""
