from fastcrud import FastCRUD

from ..models.archive import ArchiveMetadata
from ..schemas.archive import (
    ArchiveMetadataCreateInternal,
    ArchiveMetadataDelete,
    ArchiveMetadataRead,
    ArchiveMetadataUpdate,
    ArchiveMetadataUpdateInternal,
)

CRUDArchiveMetadata = FastCRUD[
    ArchiveMetadata,
    ArchiveMetadataCreateInternal,
    ArchiveMetadataDelete,
    ArchiveMetadataUpdate,
    ArchiveMetadataUpdateInternal,
    ArchiveMetadataRead,
]
crud_archive_metadata = CRUDArchiveMetadata(ArchiveMetadata)
