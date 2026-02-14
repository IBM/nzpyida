from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.core.exceptions import AzureError
from datetime import datetime, timedelta
from typing import Optional, Dict, List

from nzpyida.storage.base import CloudStorageProvider
from nzpyida.storage.exceptions import StorageError, ConfigurationError

class AzureBlobStorage(CloudStorageProvider):
    """Azure Blob Storage provider implementation"""

    REQUIRED_CONFIG = ['account_name', 'container_name']

    def __init__(self, config: Dict):
        """
        Initialize Azure Blob Storage.

        Parameters:
        -----------
        config : dict
            Configuration with keys:
            - account_name: Azure storage account name
            - container_name: Blob container name
            - account_key: Storage account key (optional)
            - connection_string: Full connection string (optional)
            - sas_token: SAS token (optional)
        """
        super().__init__(config)

        # Initialize blob service client
        if 'connection_string' in config:
            self.blob_service = BlobServiceClient.from_connection_string(
                config['connection_string']
            )
        elif 'account_key' in config:
            account_url = f"https://{config['account_name']}.blob.core.windows.net"
            self.blob_service = BlobServiceClient(
                account_url=account_url,
                credential=config['account_key']
            )
        elif 'sas_token' in config:
            account_url = f"https://{config['account_name']}.blob.core.windows.net"
            self.blob_service = BlobServiceClient(
                account_url=account_url,
                credential=config['sas_token']
            )
        else:
            raise ConfigurationError(
                "Must provide connection_string, account_key, or sas_token"
            )

        self.container_name = config['container_name']
        self.container_client = self.blob_service.get_container_client(
            self.container_name
        )

    def _validate_config(self):
        """Validate Azure configuration"""
        for key in self.REQUIRED_CONFIG:
            if key not in self.config:
                raise ConfigurationError(f"Missing required config: {key}")

        # Test container access
        try:
            self.container_client.get_container_properties()
        except AzureError as e:
            raise ConfigurationError(f"Cannot access container: {e}")

    def upload_file(self, local_path: str, remote_path: str,
               metadata: Optional[Dict] = None,
               overwrite: bool = True) -> str:
        """
        Upload file to Azure Blob Storage.

        Parameters:
        -----------
        local_path : str
            Path to local file
        remote_path : str
            Destination path in blob storage
        metadata : dict, optional
            Additional metadata
        overwrite : bool, optional
            Whether to overwrite existing file (default: True)

        Returns:
        --------
        str : Azure blob URL

        Raises:
        -------
        StorageError : If file exists and overwrite=False
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)

            # Check if file exists when overwrite=False
            if not overwrite and self.file_exists(remote_path):
                raise StorageError(
                    f"File already exists: {remote_path}. "
                    f"Set overwrite=True to replace it."
                )

            with open(local_path, 'rb') as data:
                blob_client.upload_blob(
                    data,
                    overwrite=overwrite,  # Use the parameter
                    metadata=metadata
                )

            return f"https://{self.config['account_name']}.blob.core.windows.net/{self.container_name}/{remote_path}"

        except AzureError as e:
            raise StorageError(f"Failed to upload to Azure: {e}")

    def download_file(self, remote_path: str, local_path: str):
        """Download file from Azure Blob Storage"""
        try:
            blob_client = self.container_client.get_blob_client(remote_path)

            with open(local_path, 'wb') as file:
                download_stream = blob_client.download_blob()
                file.write(download_stream.readall())

        except AzureError as e:
            raise StorageError(f"Failed to download from Azure: {e}")

    def list_files(self, prefix: str = "") -> List[str]:
        """List files in Azure container"""
        try:
            blobs = self.container_client.list_blobs(name_starts_with=prefix)
            return [blob.name for blob in blobs]

        except AzureError as e:
            raise StorageError(f"Failed to list Azure blobs: {e}")

    def delete_file(self, remote_path: str):
        """Delete file from Azure Blob Storage"""
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            blob_client.delete_blob()

        except AzureError as e:
            raise StorageError(f"Failed to delete from Azure: {e}")

    def file_exists(self, remote_path: str) -> bool:
        """Check if file exists in Azure"""
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            blob_client.get_blob_properties()
            return True
        except AzureError:
            return False

    def get_file_url(self, remote_path: str, expiry: int = 3600) -> str:
        """Generate SAS URL for Azure blob"""
        try:
            blob_client = self.container_client.get_blob_client(remote_path)

            sas_token = generate_blob_sas(
                account_name=self.config['account_name'],
                container_name=self.container_name,
                blob_name=remote_path,
                account_key=self.config.get('account_key'),
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(seconds=expiry)
            )

            return f"{blob_client.url}?{sas_token}"

        except AzureError as e:
            raise StorageError(f"Failed to generate Azure URL: {e}")
