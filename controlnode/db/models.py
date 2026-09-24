"""Modelos ORM del catalogo del ControlNode.

Cada entidad lleva domain_id (costura Hito 3: cuando lleguen objetos del otro
dominio conviven en las mismas tablas distinguidos por este campo).
"""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# --- Constantes de dominio ---


class ObjectStatus:
    PENDING = "PENDING"
    COMMITTED = "COMMITTED"
    DELETED = "DELETED"


class ReplicaStatus:
    ASSIGNED = "ASSIGNED"
    CONFIRMED = "CONFIRMED"
    LOST = "LOST"


class NodeStatus:
    ALIVE = "ALIVE"
    SUSPECTED = "SUSPECTED"
    DEAD = "DEAD"


# --- Base ---


class Base(DeclarativeBase):
    pass


# --- Users ---


class User(Base):
    __tablename__ = "users"

    user_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    username: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(256), nullable=False)
    domain_id: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[float] = mapped_column(Float, nullable=False)


# --- Directorios (namespace virtual) ---


class Directory(Base):
    __tablename__ = "directories"

    dir_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    domain_id: Mapped[str] = mapped_column(String(64), nullable=False)
    owner_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("users.user_id"), nullable=False
    )
    full_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    parent_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        UniqueConstraint("domain_id", "owner_id", "full_path", name="uq_dir_path"),
        Index("ix_dir_parent", "owner_id", "parent_path"),
    )


# --- Objetos ---


class ObjectRow(Base):
    __tablename__ = "objects"

    object_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    domain_id: Mapped[str] = mapped_column(String(64), nullable=False)
    owner_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("users.user_id"), nullable=False
    )
    object_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    total_size: Mapped[int] = mapped_column(Integer, nullable=False)
    block_size: Mapped[int] = mapped_column(Integer, nullable=False)
    global_checksum: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[float] = mapped_column(Float, nullable=False)
    committed_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    deleted_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    expires_at: Mapped[float] = mapped_column(Float, nullable=False)

    blocks: Mapped[list["Block"]] = relationship(
        back_populates="object", cascade="all, delete-orphan"
    )

    __table_args__ = (
        # Indice unico parcial: solo COMMITTED debe ser unico por
        # (owner, key, version). Los PENDING coexisten con la misma key
        # mientras se decide su version en el commit (asignada atomicamente).
        Index(
            "uq_obj_committed_version",
            "domain_id",
            "owner_id",
            "object_key",
            "version",
            unique=True,
            sqlite_where=text("status = 'COMMITTED'"),
        ),
        CheckConstraint(
            "status IN ('PENDING','COMMITTED','DELETED')",
            name="ck_obj_status",
        ),
        Index("ix_obj_owner_key_status", "owner_id", "object_key", "status"),
        Index("ix_obj_status_expiry", "status", "expires_at"),
    )


class Block(Base):
    __tablename__ = "blocks"

    block_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    object_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("objects.object_id"), nullable=False
    )
    block_index: Mapped[int] = mapped_column(Integer, nullable=False)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    checksum: Mapped[str] = mapped_column(String(128), nullable=False)

    object: Mapped["ObjectRow"] = relationship(back_populates="blocks")
    replicas: Mapped[list["Replica"]] = relationship(
        back_populates="block", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("object_id", "block_index", name="uq_block_index"),
    )


class Replica(Base):
    __tablename__ = "replicas"

    replica_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    block_id: Mapped[str] = mapped_column(
        String(80), ForeignKey("blocks.block_id"), nullable=False
    )
    datanode_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("datanodes.node_id"), nullable=False
    )
    replica_index: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    assigned_at: Mapped[float] = mapped_column(Float, nullable=False)
    confirmed_at: Mapped[float | None] = mapped_column(Float, nullable=True)

    block: Mapped["Block"] = relationship(back_populates="replicas")
    datanode: Mapped["DataNode"] = relationship()

    __table_args__ = (
        UniqueConstraint("block_id", "datanode_id", name="uq_replica_block_node"),
        CheckConstraint(
            "status IN ('ASSIGNED','CONFIRMED','LOST')",
            name="ck_replica_status",
        ),
        Index("ix_replica_node_status", "datanode_id", "status"),
    )


# --- DataNodes ---


class DataNode(Base):
    __tablename__ = "datanodes"

    node_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    domain_id: Mapped[str] = mapped_column(String(64), nullable=False)
    address: Mapped[str] = mapped_column(String(256), nullable=False)
    free_bytes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    load_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    last_heartbeat_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    registered_at: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "status IN ('ALIVE','SUSPECTED','DEAD')",
            name="ck_node_status",
        ),
        Index("ix_node_status", "status"),
    )


# --- Log de eventos (costura Hito 3) ---


class Event(Base):
    __tablename__ = "events"

    event_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    domain_id: Mapped[str] = mapped_column(String(64), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(80), nullable=False)
    payload: Mapped[str] = mapped_column(Text, nullable=False)  # JSON
    created_at: Mapped[float] = mapped_column(Float, nullable=False)
    propagated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("ix_event_prop", "propagated", "event_id"),
    )


# Catalogo de tipos de evento (referencia).
class EventType:
    OBJECT_INIT = "OBJECT_INIT"
    OBJECT_COMMITTED = "OBJECT_COMMITTED"
    OBJECT_DELETED = "OBJECT_DELETED"
    OBJECT_PENDING_EXPIRED = "OBJECT_PENDING_EXPIRED"
    REPLICA_ASSIGNED = "REPLICA_ASSIGNED"
    REPLICA_CONFIRMED = "REPLICA_CONFIRMED"
    REPLICA_LOST = "REPLICA_LOST"
    DATANODE_REGISTERED = "DATANODE_REGISTERED"
    DATANODE_HEARTBEAT = "DATANODE_HEARTBEAT"
    DATANODE_SUSPECTED = "DATANODE_SUSPECTED"
    DATANODE_DEAD = "DATANODE_DEAD"
    DATANODE_RECOVERED = "DATANODE_RECOVERED"
    NAMESPACE_MKDIR = "NAMESPACE_MKDIR"
    NAMESPACE_RMDIR = "NAMESPACE_RMDIR"
    USER_REGISTERED = "USER_REGISTERED"
