from contextlib import asynccontextmanager
import os
from pathlib import Path
import shutil

from typing import AsyncIterator, BinaryIO, Tuple

from open_webui.constants import ERROR_MESSAGES
from open_webui.config import UPLOAD_DIR
from open_webui.storage.base_storage_provider import StorageProvider

class LocalStorageProvider(StorageProvider):
    async def upload_file(self, file: BinaryIO, filename: str) -> Tuple[bytes, str]:
        """Uploads a file to the local file system."""
        contents = file.read()
        if not contents:
            raise ValueError(ERROR_MESSAGES.EMPTY_CONTENT)

        file_path = Path(UPLOAD_DIR) / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "wb") as f:
            f.write(contents)
        return contents, file_path.as_posix()

    async def get_file(self, file_path: str) -> AsyncIterator[bytes]:
        chunk_size = 8 * 1024
        with open(file_path, "rb") as file:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    @asynccontextmanager
    async def as_local_file(self, file_path: str) -> AsyncIterator[str]:
        yield file_path

    async def delete_file(self, filename: str) -> None:
        """Deletes a file from the local file system."""
        file_path = f"{UPLOAD_DIR}/{filename}"
        if os.path.isfile(file_path):
            os.remove(file_path)
        else:
            print(f"File {file_path} not found in local storage.")

    async def delete_all_files(self) -> None:
        """Deletes all files from the storage."""
        if os.path.exists(UPLOAD_DIR):
            for filename in os.listdir(UPLOAD_DIR):
                file_path = os.path.join(UPLOAD_DIR, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)  # Remove the file or link
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)  # Remove the directory
                except Exception as e:
                    print(f"Failed to delete {file_path}. Reason: {e}")
        else:
            print(f"Directory {UPLOAD_DIR} not found in local storage.")
