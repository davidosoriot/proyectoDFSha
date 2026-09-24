"""Esquemas Pydantic para requests y responses de la API REST.

Ningun esquema transporta bytes de contenido. Los bodys aceptan solo metadatos
(checksums, tamanos, block_ids) y las respuestas devuelven ubicaciones
(node_id, address), nunca datos de los bloques.
"""

from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, Field


# --- Auth ---


class RegisterRequest(BaseModel):
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=4, max_length=256)


class RegisterResponse(BaseModel):
    user_id: str
    username: str


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    expires_in: int


# --- Namespace ---


class MkdirRequest(BaseModel):
    path: str


class MkdirResponse(BaseModel):
    dir_id: str
    path: str


class LsEntry(BaseModel):
    name: str
    type: Literal["dir", "object"]
    created_at: float


class LsResponse(BaseModel):
    path: str
    entries: List[LsEntry]


# --- Objects ---


class BlockManifestEntry(BaseModel):
    block_id: str
    index: int
    size: int
    checksum: str


class InitObjectRequest(BaseModel):
    object_key: str
    total_size: int
    block_size: int
    global_checksum: str
    blocks: List[BlockManifestEntry]


class DataNodeLocation(BaseModel):
    node_id: str
    address: str


class BlockPlacementInfo(BaseModel):
    block_id: str
    index: int
    replica_nodes: List[DataNodeLocation]


class InitObjectResponse(BaseModel):
    object_id: str
    status: str
    placements: List[BlockPlacementInfo]


class CommitObjectResponse(BaseModel):
    object_id: str
    version: int
    status: str


class BlockReadInfo(BaseModel):
    block_id: str
    index: int
    size: int
    checksum: str
    replicas: List[DataNodeLocation]


class GetObjectResponse(BaseModel):
    object_id: str
    object_key: str
    version: int
    total_size: int
    block_size: int
    global_checksum: str
    blocks: List[BlockReadInfo]


class ObjectVersionInfo(BaseModel):
    object_id: str
    version: int
    status: str
    total_size: int
    created_at: float
    committed_at: Optional[float] = None
    deleted_at: Optional[float] = None


class StatResponse(BaseModel):
    object_key: str
    versions: List[ObjectVersionInfo]


class ReportFailureRequest(BaseModel):
    node_id: str
    reason: str


class ReportFailureResponse(BaseModel):
    acknowledged: bool
    replica_id: str


# --- DataNodes ---


class RegisterDataNodeRequest(BaseModel):
    node_id: str
    domain_id: str
    address: str
    free_bytes: int
    load_score: float = 0.0


class RegisterDataNodeResponse(BaseModel):
    registered: bool
    registered_at: float


class HeartbeatRequest(BaseModel):
    free_bytes: int
    load_score: float


class HeartbeatResponse(BaseModel):
    ok: bool


class BlockReportEntry(BaseModel):
    block_id: str
    checksum: str


class BlockReportRequest(BaseModel):
    blocks: List[BlockReportEntry]


class BlockReportResponse(BaseModel):
    unknown_blocks: List[str]
