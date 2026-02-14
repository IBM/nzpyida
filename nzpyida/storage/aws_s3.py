import boto3
from botocore.exceptions import ClientError
from pathlib import Path
from typing import Optional, Dict, List

from nzpyida.storage.base import CloudStorageProvider
from nzpyida.storage.exceptions import StorageError, ConfigurationError

class AWSS3Storage(CloudStorageProvider):
    """AWS S3 storage provider implementation"""

    REQUIRED_CONFIG = ['bucket_name', 'region']

    def __init__(self, config: Dict):
        """
        Initialize AWS S3 storage.

        Parameters:
        -----------
        config : dict
            Configuration with keys:
            - bucket_name: S3 bucket name
            - region: AWS region (e.g., 'us-east-1')
            - access_key_id: AWS access key (optional, uses default credentials)
            - secret_access_key: AWS secret key (optional)
            - session_token: AWS session token (optional)
        """
        print("Before init")
        super().__init__(config)
        print("After init")

        # Initialize S3 client
        session_config = {}
        if 'access_key_id' in config:
            session_config['aws_access_key_id'] = config['access_key_id']
        if 'secret_access_key' in config:
            session_config['aws_secret_access_key'] = config['secret_access_key']
        if 'session_token' in config:
            session_config['aws_session_token'] = config['session_token']
        # print(f"Session config: {session_config} and region_name : {}")
        
        self.s3_client = boto3.client(
            's3',
            region_name=config['region'],
            **session_config
        )
        self.bucket_name = config['bucket_name']
        self._validate_config()

    def _validate_config(self):
        """Validate AWS S3 configuration"""
        for key in self.REQUIRED_CONFIG:
            if key not in self.config:
                raise ConfigurationError(f"Missing required config: {key}")

        # Test bucket access
        try:
            self.s3_client.head_bucket(Bucket=self.config['bucket_name'])
            print(f"Bucket {self.config['bucket_name']} exists")
        except ClientError as e:
            raise ConfigurationError(f"Cannot access bucket: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error accessing bucket: {e}")

    def upload_file(self, local_path: str, remote_path: str,
               metadata: Optional[Dict] = None, 
               overwrite: bool = True) -> str:
        """
        Upload file to S3.

        Parameters:
        -----------
        local_path : str
            Path to local file
        remote_path : str
            Destination path in S3
        metadata : dict, optional
            Additional metadata
        overwrite : bool, optional
            Whether to overwrite existing file (default: True)

        Returns:
        --------
        str : S3 URL

        Raises:
        -------
        StorageError : If file exists and overwrite=False
        """
        try:
            # Check if file exists when overwrite=False
            if not overwrite and self.file_exists(remote_path):
                raise StorageError(
                    f"File already exists: s3://{self.bucket_name}/{remote_path}. "
                    f"Set overwrite=True to replace it."
                )

            extra_args = {}
            if metadata:
                extra_args['Metadata'] = metadata

            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                remote_path,
                ExtraArgs=extra_args
            )

            return f"s3://{self.bucket_name}/{remote_path}"

        except ClientError as e:
            raise StorageError(f"Failed to upload to S3: {e}")

    def download_file(self, remote_path: str, local_path: str):
        """Download file from S3"""
        try:
            self.s3_client.download_file(
                self.bucket_name,
                remote_path,
                local_path
            )
        except ClientError as e:
            raise StorageError(f"Failed to download from S3: {e}")

    def list_files(self, prefix: str = "") -> List[str]:
        """List files in S3 bucket"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            if 'Contents' not in response:
                return []

            return [obj['Key'] for obj in response['Contents']]

        except ClientError as e:
            raise StorageError(f"Failed to list S3 files: {e}")

    def delete_file(self, remote_path: str):
        """Delete file from S3"""
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=remote_path
            )
        except ClientError as e:
            raise StorageError(f"Failed to delete from S3: {e}")

    def file_exists(self, remote_path: str) -> bool:
        """Check if file exists in S3"""
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=remote_path
            )
            return True
        except ClientError:
            return False

    def get_file_url(self, remote_path: str, expiry: int = 3600) -> str:
        """Generate presigned URL for S3 object"""
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': remote_path
                },
                ExpiresIn=expiry
            )
            return url
        except ClientError as e:
            raise StorageError(f"Failed to generate S3 URL: {e}")
