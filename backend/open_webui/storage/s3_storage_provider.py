from contextlib import asynccontextmanager
from tempfile import NamedTemporaryFile
from typing import AsyncIterator, BinaryIO, Tuple, TYPE_CHECKING

import aioboto3

from open_webui.constants import ERROR_MESSAGES
from open_webui.config import (
    S3_BUCKET_PREFIX,
    S3_ACCESS_KEY_ID,
    S3_SECRET_ACCESS_KEY,
    S3_BUCKET_NAME,
    S3_REGION_NAME,
    S3_ENDPOINT_URL,
)

from open_webui.storage.base_storage_provider import StorageProvider

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client

class S3StorageProvider(StorageProvider):
    def __init__(self):
        self.session = aioboto3.Session()
        assert S3_BUCKET_NAME, "S3_BUCKET_NAME must be set if using S3StorageProvider"
        self.bucket_name: str = S3_BUCKET_NAME
        self.bucket_prefix: str = S3_BUCKET_PREFIX or ""

    def _get_client(self) -> "S3Client":
        return self.session.client(
            "s3",
            region_name=S3_REGION_NAME,
            endpoint_url=S3_ENDPOINT_URL,
            aws_access_key_id=S3_ACCESS_KEY_ID,
            aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        )

    async def upload_file(self, file: BinaryIO, filename: str) -> Tuple[bytes, str]:
        """Uploads a file to S3."""
        contents = file.read()
        if not contents:
            raise ValueError(ERROR_MESSAGES.EMPTY_CONTENT)

        try:
            async with self._get_client() as client:
                await client.put_object(
                    Bucket=self.bucket_name,
                    Key=f"{self.bucket_prefix}/{filename}",
                    Body=contents,
                )
            return contents, f"s3://{self.bucket_name}/{self.bucket_prefix}/{filename}"
        except Exception as e:
            raise RuntimeError(f"Error uploading file to S3: {e}")

    async def get_file(self, path: str) -> AsyncIterator[bytes]:
        """Downloads a file from S3 and returns the local file path."""
        try:
            bucket_name, key = self._bucket_name_and_key(path)
            async with self._get_client() as client:
                response = await client.get_object(Bucket=bucket_name, Key=key)
                async for chunk in response["Body"].iter_chunks(
                    self.STREAMING_CHUNK_SIZE
                ):
                    yield chunk
        except Exception as e:
            raise RuntimeError(f"Error downloading file {path} from S3: {e}")

    @asynccontextmanager
    async def as_local_file(self, path: str) -> AsyncIterator[str]:
        try:
            bucket_name, key = self._bucket_name_and_key(path)
            with NamedTemporaryFile() as f:
                async with self._get_client() as client:
                    await client.download_fileobj(bucket_name, key, f)
                yield f.name
                print(f"download s3 file to {f.name}")
        except Exception as e:
            raise RuntimeError(f"Error downloading file {path} from S3: {e}")

    async def delete_file(self, path: str) -> None:
        """Deletes a file from S3."""
        try:
            async with self._get_client() as client:
                await client.delete_object(Bucket=self.bucket_name, Key=path)
        except Exception as e:
            raise RuntimeError(f"Error deleting file {path} from S3: {e}")

    async def delete_all_files(self) -> None:
        """Deletes all files from S3."""
        try:
            async with self.session.resource(
                "s3",
                region_name=S3_REGION_NAME,
                endpoint_url=S3_ENDPOINT_URL,
                aws_access_key_id=S3_ACCESS_KEY_ID,
                aws_secret_access_key=S3_SECRET_ACCESS_KEY,
            ) as client:
                bucket = await client.Bucket(self.bucket_name)
                await bucket.objects.filter(Prefix=self.bucket_prefix).delete()
        except Exception as e:
            raise RuntimeError(f"Error deleting all files from S3: {e}")

    def _bucket_name_and_key(self, s3_path: str) -> Tuple[str, str]:
        bucket_name, key = s3_path.split("s3://")[1].split("/", 1)
        return bucket_name, key
