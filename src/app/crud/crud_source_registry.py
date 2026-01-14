from fastcrud import FastCRUD

from ..models.registry import SourceRegistry
from ..schemas.registry import (
    SourceRegistryCreateInternal,
    SourceRegistryDelete,
    SourceRegistryRead,
    SourceRegistryUpdate,
    SourceRegistryUpdateInternal,
)

CRUDSourceRegistry = FastCRUD[
    SourceRegistry,
    SourceRegistryCreateInternal,
    SourceRegistryDelete,
    SourceRegistryUpdate,
    SourceRegistryUpdateInternal,
    SourceRegistryRead,
]
crud_source_registry = CRUDSourceRegistry(SourceRegistry)

