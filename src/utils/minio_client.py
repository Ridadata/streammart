"""
MinIO Client Utility
Reusable MinIO/S3 connection and operations
"""

from minio import Minio
from minio.error import S3Error
import os
from dotenv import load_dotenv
import logging
from datetime import timedelta

load_dotenv()

logger = logging.getLogger(__name__)


class MinIOClient:
    """
    MinIO client for S3-compatible operations
    
    Usage:
        client = MinIOClient()
        
        # List objects
        objects = client.list_objects('raw-events')
        
        # Upload file
        client.upload_file('raw-events', 'data.parquet', '/path/to/file')
        
        # Download file
        client.download_file('raw-events', 'data.parquet', '/path/to/save')
    """
    
    def __init__(self):
        endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        access_key = os.getenv('MINIO_ACCESS_KEY')
        secret_key = os.getenv('MINIO_SECRET_KEY')

        if not access_key or not secret_key:
            raise RuntimeError(
                "MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables must be set. "
                "Refusing to fall back to hardcoded default credentials."
            )

        # Remove http:// if present
        endpoint = endpoint.replace('http://', '').replace('https://', '')
        
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False  # Use HTTP (not HTTPS) for local development
        )
        
        logger.info(f"MinIO client initialized: {endpoint}")
    
    def bucket_exists(self, bucket_name):
        """Check if bucket exists"""
        try:
            return self.client.bucket_exists(bucket_name)
        except S3Error as e:
            logger.error(f"Error checking bucket: {e}")
            return False
    
    def create_bucket(self, bucket_name):
        """Create bucket if it doesn't exist"""
        try:
            if not self.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Bucket created: {bucket_name}")
            else:
                logger.info(f"Bucket already exists: {bucket_name}")
        except S3Error as e:
            logger.error(f"Error creating bucket: {e}")
            raise
    
    def list_buckets(self):
        """List all buckets"""
        try:
            buckets = self.client.list_buckets()
            return [bucket.name for bucket in buckets]
        except S3Error as e:
            logger.error(f"Error listing buckets: {e}")
            return []
    
    def list_objects(self, bucket_name, prefix='', recursive=True):
        """
        List objects in bucket
        
        Args:
            bucket_name: Bucket name
            prefix: Object name prefix filter
            recursive: List recursively
        
        Returns:
            List of object names
        """
        try:
            objects = self.client.list_objects(
                bucket_name,
                prefix=prefix,
                recursive=recursive
            )
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects: {e}")
            return []
    
    def upload_file(self, bucket_name, object_name, file_path):
        """Upload file to bucket"""
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            logger.info(f"Uploaded: {file_path} -> {bucket_name}/{object_name}")
        except S3Error as e:
            logger.error(f"Error uploading file: {e}")
            raise
    
    def download_file(self, bucket_name, object_name, file_path):
        """Download file from bucket"""
        try:
            self.client.fget_object(bucket_name, object_name, file_path)
            logger.info(f"Downloaded: {bucket_name}/{object_name} -> {file_path}")
        except S3Error as e:
            logger.error(f"Error downloading file: {e}")
            raise
    
    def delete_object(self, bucket_name, object_name):
        """Delete object from bucket"""
        try:
            self.client.remove_object(bucket_name, object_name)
            logger.info(f"Deleted: {bucket_name}/{object_name}")
        except S3Error as e:
            logger.error(f"Error deleting object: {e}")
            raise
    
    def get_object_stats(self, bucket_name, object_name):
        """Get object metadata"""
        try:
            stats = self.client.stat_object(bucket_name, object_name)
            return {
                'size': stats.size,
                'last_modified': stats.last_modified,
                'etag': stats.etag,
                'content_type': stats.content_type
            }
        except S3Error as e:
            logger.error(f"Error getting object stats: {e}")
            return None
    
    def get_presigned_url(self, bucket_name, object_name, expires=timedelta(hours=1)):
        """
        Generate presigned URL for temporary access
        
        Useful for sharing data with external tools
        """
        try:
            url = self.client.presigned_get_object(
                bucket_name,
                object_name,
                expires=expires
            )
            return url
        except S3Error as e:
            logger.error(f"Error generating presigned URL: {e}")
            return None
    
    def get_bucket_size(self, bucket_name):
        """Calculate total size of all objects in bucket"""
        try:
            objects = self.client.list_objects(bucket_name, recursive=True)
            total_size = sum(obj.size for obj in objects)
            return total_size
        except S3Error as e:
            logger.error(f"Error calculating bucket size: {e}")
            return 0


# Singleton instance
_minio_client_instance = None

def get_minio_client():
    """Get shared MinIO client instance"""
    global _minio_client_instance
    if _minio_client_instance is None:
        _minio_client_instance = MinIOClient()
    return _minio_client_instance
