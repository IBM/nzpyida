from typing import Optional, Dict, Union
from pathlib import Path

from nzpyida.storage.base import CloudStorageProvider
from nzpyida.storage.aws_s3 import AWSS3Storage
from nzpyida.storage.azure_blob import AzureBlobStorage
from nzpyida.storage.exceptions import StorageError, ConfigurationError

class StagingManager:
    """
    High-level API for managing file staging to cloud storage with default directory support.
    """

    PROVIDERS = {
        'aws_s3': AWSS3Storage,
        'azure_blob': AzureBlobStorage,
        # Future providers
        # 'ibm_cos': IBMCOSStorage,
        # 'gcp_storage': GCPStorage,
    }

    def __init__(self, provider: str, config: Dict, default_directory: str = "nzpyida-staging"):
        """
        Initialize staging manager with default directory support.

        Parameters:
        -----------
        provider : str
            Cloud provider name ('aws_s3', 'azure_blob', etc.)
        config : dict
            Provider-specific configuration
        default_directory : str, optional
            Default directory/prefix for all operations (default: 'nzpyida-staging')
            Set to empty string '' to disable default directory

        Examples:
        ---------
        # AWS S3 with default directory
        staging = StagingManager('aws_s3', {
            'bucket_name': 'my-netezza-staging',
            'region': 'us-east-1',
            'access_key_id': 'my-access-key-id',
            'secret_access_key': 'my-secret-access-key'
        }, default_directory='my-project/staging')

        # Azure Blob with default directory
        staging = StagingManager('azure_blob', {
            'account_name': 'mystorageaccount',
            'container_name': 'netezza-staging',
            'account_key': 'your-account-key'
        }, default_directory='data/staging')

        # No default directory
        staging = StagingManager('aws_s3', config, default_directory='')
        """
        print(f"provider : {provider} and conig : {config}'")
        if provider not in self.PROVIDERS:
            raise ConfigurationError(
                f"Unknown provider: {provider}. "
                f"Available: {list(self.PROVIDERS.keys())}"
            )

        self.provider_name = provider
        print(f"provider : {provider} and conig : {config}'")
        self.storage: CloudStorageProvider = self.PROVIDERS[provider](config)

        # Set default directory (remove trailing slash if present)
        self.default_directory = default_directory.rstrip('/') if default_directory else ''

    def _build_path(self, path: str, use_default: bool = True) -> str:
        """
        Build full path with default directory if enabled.

        Parameters:
        -----------
        path : str
            Relative path
        use_default : bool
            Whether to prepend default directory (default: True)

        Returns:
        --------
        str : Full path with default directory prepended if applicable
        """
        if not use_default or not self.default_directory:
            return path

        # Remove leading slash from path if present
        path = path.lstrip('/')

        # Combine default directory with path
        if path:
            return f"{self.default_directory}/{path}"
        else:
            return self.default_directory

    def upload(self, local_path: str, remote_path: Optional[str] = None,
          metadata: Optional[Dict] = None, 
          use_default_dir: bool = True,
          overwrite: bool = True) -> str:
        """
        Upload a file to cloud staging area.

        Parameters:
        -----------
        local_path : str
            Path to local file
        remote_path : str, optional
            Destination path (defaults to filename)
            If use_default_dir=True, this is relative to default_directory
        metadata : dict, optional
            Additional metadata
        use_default_dir : bool, optional
            Whether to use default directory (default: True)
        overwrite : bool, optional
            Whether to overwrite existing file (default: True)
            If False and file exists, raises StorageError

        Returns:
        --------
        str : Cloud storage URL
        
        Raises:
        -------
        StorageError : If file exists and overwrite=False
        
        Examples:
        ---------
        # Upload and overwrite if exists (default)
        staging.upload('data.csv')
        
        # Fail if file already exists
        try:
            staging.upload('data.csv', overwrite=False)
        except StorageError as e:
            print(f"File exists: {e}")
        
        # Check before upload
        if not staging.exists('data.csv'):
            staging.upload('data.csv')
        else:
            print("File already exists, skipping upload")
        """
        if remote_path is None:
            remote_path = Path(local_path).name
        
        # Build full path with default directory
        full_path = self._build_path(remote_path, use_default_dir)

        return self.storage.upload_file(local_path, full_path, metadata, overwrite)

    def download(self, remote_path: str, local_path: Optional[str] = None,
                use_default_dir: bool = True):
        """
        Download a file from cloud staging area.

        Parameters:
        -----------
        remote_path : str
            Path to file in cloud storage
            If use_default_dir=True, this is relative to default_directory
        local_path : str, optional
            Local destination path (defaults to filename)
        use_default_dir : bool, optional
            Whether to use default directory (default: True)
        """
        if local_path is None:
            local_path = Path(remote_path).name

        # Build full path with default directory
        full_path = self._build_path(remote_path, use_default_dir)

        self.storage.download_file(full_path, local_path)

    def list(self, prefix: str = "", use_default_dir: bool = True):
        """
        List files in staging area.

        Parameters:
        -----------
        prefix : str, optional
            Prefix to filter files
            If use_default_dir=True, this is relative to default_directory
        use_default_dir : bool, optional
            Whether to use default directory (default: True)

        Returns:
        --------
        list : List of file paths

        Examples:
        ---------
        # List all files in default directory
        files = staging.list()

        # List files in subdirectory within default
        files = staging.list('raw/')

        # List all files in bucket (bypass default directory)
        files = staging.list('', use_default_dir=False)
        """
        # Build full prefix with default directory
        full_prefix = self._build_path(prefix, use_default_dir)

        return self.storage.list_files(full_prefix)

    def delete(self, remote_path: str, use_default_dir: bool = True):
        """
        Delete a file from staging area.

        Parameters:
        -----------
        remote_path : str
            Path to file in cloud storage
            If use_default_dir=True, this is relative to default_directory
        use_default_dir : bool, optional
            Whether to use default directory (default: True)
        """
        # Build full path with default directory
        full_path = self._build_path(remote_path, use_default_dir)

        self.storage.delete_file(full_path)

    def exists(self, remote_path: str, use_default_dir: bool = True) -> bool:
        """
        Check if file exists in staging area.

        Parameters:
        -----------
        remote_path : str
            Path to file in cloud storage
            If use_default_dir=True, this is relative to default_directory
        use_default_dir : bool, optional
            Whether to use default directory (default: True)

        Returns:
        --------
        bool : True if file exists, False otherwise
        """
        # Build full path with default directory
        full_path = self._build_path(remote_path, use_default_dir)

        return self.storage.file_exists(full_path)

    def get_url(self, remote_path: str, expiry: int = 3600, 
               use_default_dir: bool = True) -> str:
        """
        Get temporary access URL for file.

        Parameters:
        -----------
        remote_path : str
            Path to file in cloud storage
            If use_default_dir=True, this is relative to default_directory
        expiry : int, optional
            URL expiry time in seconds (default: 3600 = 1 hour)
        use_default_dir : bool, optional
            Whether to use default directory (default: True)

        Returns:
        --------
        str : Temporary signed URL
        """
        # Build full path with default directory
        full_path = self._build_path(remote_path, use_default_dir)

        return self.storage.get_file_url(full_path, expiry)

    def get_default_directory(self) -> str:
        """
        Get the current default directory.

        Returns:
        --------
        str : Default directory path
        """
        return self.default_directory

    def set_default_directory(self, directory: str):
        """
        Change the default directory.

        Parameters:
        -----------
        directory : str
            New default directory path
            Set to empty string '' to disable default directory
        """
        self.default_directory = directory.rstrip('/') if directory else ''
