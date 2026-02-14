from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from pathlib import Path

class CloudStorageProvider(ABC):
    """
    Abstract base class for cloud storage providers.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize storage provider with configuration.

        Parameters:
        -----------
        config : dict
            Provider-specific configuration
        """
        self.config = config
        # self._validate_config()

    @abstractmethod
    def _validate_config(self):
        """Validate provider-specific configuration"""
        pass

    @abstractmethod
    def upload_file(self, local_path: str, remote_path: str, 
                   metadata: Optional[Dict] = None, 
                   overwrite: bool = True) -> str:
        """
        Upload a file to cloud storage.

        Parameters:
        -----------
        local_path : str
            Path to local file
        remote_path : str
            Destination path in cloud storage
        metadata : dict, optional
            Additional metadata to attach
        overwrite : bool, optional
            Whether to overwrite existing file (default: True)
            If False and file exists, raises StorageError

        Returns:
        --------
        str : URL or identifier of uploaded file

        Raises:
        -------
        StorageError : If file exists and overwrite=False
        """
        pass

    @abstractmethod
    def download_file(self, remote_path: str, local_path: str):
        """Download a file from cloud storage"""
        pass

    @abstractmethod
    def list_files(self, prefix: str = "") -> List[str]:
        """List files in cloud storage with optional prefix"""
        pass

    @abstractmethod
    def delete_file(self, remote_path: str):
        """Delete a file from cloud storage"""
        pass

    @abstractmethod
    def file_exists(self, remote_path: str) -> bool:
        """Check if file exists in cloud storage"""
        pass

    @abstractmethod
    def get_file_url(self, remote_path: str, expiry: int = 3600) -> str:
        """
        Get a temporary signed URL for file access.

        Parameters:
        -----------
        remote_path : str
            Path to file in cloud storage
        expiry : int
            URL expiry time in seconds (default: 1 hour)

        Returns:
        --------
        str : Signed URL
        """
        pass
