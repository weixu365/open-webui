from abc import ABC, abstractmethod
from typing import AsyncIterator, BinaryIO, Tuple, BinaryIO, Tuple, AsyncContextManager


class StorageProvider(ABC):
    @abstractmethod
    async def upload_file(self, file: BinaryIO, path: str) -> Tuple[bytes, str]:
        """Uploads a file to the storage and returns the file content bytes and path."""

    @abstractmethod
    async def get_file(self, path: str) -> AsyncIterator[bytes]:
        """Read the contents of a file in the storage."""

    @abstractmethod
    def as_local_file(self, path: str) -> AsyncContextManager[str]:
        """Get the local file path for a file in the storage."""

    @abstractmethod
    async def delete_file(self, path: str) -> None:
        """Deletes a file from the storage."""

    @abstractmethod
    async def delete_all_files(self) -> None:
        """Deletes all files from the storage."""
