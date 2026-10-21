"""CLI de DFSha.

Uso:
    python -m client.cli <comando> [args]

Comandos:
    register <username> <password>
    login    <username> <password>              -> imprime y guarda token en ~/.dfsha_token
    mkdir    <path>
    rmdir    <path>
    ls       <path>
    put      <local_file> <remote_key>
    get      <remote_key> <local_file>
    stat     <object_id>
    delete   <object_id>

Variables de entorno:
    DFSHA_CN_URL     ControlNode (default: http://localhost:8000)
    DFSHA_TOKEN      token JWT (alternativa a ~/.dfsha_token)
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from .sdk import DFShaClient, DFShaClientError


_TOKEN_PATH = Path.home() / ".dfsha_token"


def _load_token() -> str | None:
    env = os.environ.get("DFSHA_TOKEN")
    if env:
        return env
    if _TOKEN_PATH.is_file():
        return _TOKEN_PATH.read_text(encoding="utf-8").strip() or None
    return None


def _save_token(token: str) -> None:
    _TOKEN_PATH.write_text(token, encoding="utf-8")


def _make_client() -> DFShaClient:
    url = os.environ.get("DFSHA_CN_URL", "http://localhost:8000")
    return DFShaClient(controlnode_url=url, token=_load_token())


async def _cmd_register(args: list[str]) -> int:
    if len(args) != 2:
        print("uso: register <username> <password>", file=sys.stderr)
        return 2
    c = _make_client()
    try:
        info = await c.register(args[0], args[1])
    except DFShaClientError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"registrado: user_id={info['user_id']} username={info['username']}")
    return 0


async def _cmd_login(args: list[str]) -> int:
    if len(args) != 2:
        print("uso: login <username> <password>", file=sys.stderr)
        return 2
    c = _make_client()
    try:
        token = await c.login(args[0], args[1])
    except DFShaClientError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    _save_token(token)
    print(f"token guardado en {_TOKEN_PATH}")
    return 0


async def _cmd_mkdir(args: list[str]) -> int:
    if len(args) != 1:
        print("uso: mkdir <path>", file=sys.stderr)
        return 2
    c = _make_client()
    try:
        info = await c.mkdir(args[0])
    except DFShaClientError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"creado: {info['path']} (dir_id={info['dir_id']})")
    return 0


async def _cmd_rmdir(args: list[str]) -> int:
    if len(args) != 1:
        print("uso: rmdir <path>", file=sys.stderr)
        return 2
    c = _make_client()
    try:
        await c.rmdir(args[0])
    except DFShaClientError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"borrado: {args[0]}")
    return 0


async def _cmd_ls(args: list[str]) -> int:
    if len(args) != 1:
        print("uso: ls <path>", file=sys.stderr)
        return 2
    c = _make_client()
    try:
        entries = await c.ls(args[0])
    except DFShaClientError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    for e in entries:
        marker = "/" if e["type"] == "dir" else " "
        print(f"{marker} {e['name']}")
    return 0


async def _cmd_put(args: list[str]) -> int:
    if len(args) != 2:
        print("uso: put <local_file> <remote_key>", file=sys.stderr)
        return 2
    local, remote = args
    if not Path(local).is_file():
        print(f"error: no existe {local}", file=sys.stderr)
        return 1
    c = _make_client()
    try:
        info = await c.put(local, remote)
    except DFShaClientError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"subido: object_id={info['object_id']} version={info['version']}")
    return 0


async def _cmd_get(args: list[str]) -> int:
    if len(args) != 2:
        print("uso: get <remote_key> <local_file>", file=sys.stderr)
        return 2
    remote, local = args
    c = _make_client()
    try:
        data = await c.get(remote)
    except DFShaClientError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    Path(local).write_bytes(data)
    print(f"descargado: {local} ({len(data)} bytes)")
    return 0


async def _cmd_stat(args: list[str]) -> int:
    if len(args) != 1:
        print("uso: stat <object_id>", file=sys.stderr)
        return 2
    c = _make_client()
    try:
        info = await c.stat(args[0])
    except DFShaClientError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"object_key: {info['object_key']}")
    for v in info["versions"]:
        print(
            f"  v{v['version']} {v['status']} size={v['total_size']} "
            f"created_at={v['created_at']:.0f} committed_at={v.get('committed_at')}"
        )
    return 0


async def _cmd_delete(args: list[str]) -> int:
    if len(args) != 1:
        print("uso: delete <object_id>", file=sys.stderr)
        return 2
    c = _make_client()
    try:
        await c.delete(args[0])
    except DFShaClientError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"borrado: {args[0]}")
    return 0


_COMMANDS = {
    "register": _cmd_register,
    "login": _cmd_login,
    "mkdir": _cmd_mkdir,
    "rmdir": _cmd_rmdir,
    "ls": _cmd_ls,
    "put": _cmd_put,
    "get": _cmd_get,
    "stat": _cmd_stat,
    "delete": _cmd_delete,
}


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if not argv:
        print(__doc__)
        return 2
    cmd, *rest = argv
    handler = _COMMANDS.get(cmd)
    if handler is None:
        print(f"comando desconocido: {cmd}\n", file=sys.stderr)
        print(__doc__)
        return 2
    return asyncio.run(handler(rest))


if __name__ == "__main__":
    sys.exit(main())
