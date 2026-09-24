"""Pruebas del ciclo de vida de objetos: init, commit, get, delete, aislamiento."""

from __future__ import annotations

import uuid

import pytest


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _manifest(object_key="/f.bin", n_blocks=2, block_size=64):
    """Genera un manifiesto sintetico coherente con el formato de dfshacore."""
    object_id = str(uuid.uuid4())
    blocks = [
        {
            "block_id": f"{object_id}_{i:06d}",
            "index": i,
            "size": block_size,
            "checksum": "a" * 64,
        }
        for i in range(n_blocks)
    ]
    return {
        "object_key": object_key,
        "total_size": n_blocks * block_size,
        "block_size": block_size,
        "global_checksum": "b" * 64,
        "blocks": blocks,
    }


async def _confirm_all_replicas(client, datanode_headers, object_id, blocks):
    """Simula que los DataNodes reportan que recibieron todos los bloques."""
    # Obtener la lista de replicas asignadas por bloque via GET del catalogo.
    # Como no exponemos ese endpoint, hackeamos leyendo del state via la
    # invocacion directa al init response (que el llamador debe traer).
    # Aca simplemente hacemos un block-report desde cada uno de los nodos
    # registrados con los block_ids del objeto.
    for block_id in blocks:
        # Los 3 datanodes del fixture reportan.
        for node in ["dn-1", "dn-2", "dn-3"]:
            await client.post(
                f"/datanodes/{node}/blocks",
                headers=datanode_headers,
                json={"blocks": [{"block_id": block_id, "checksum": "a" * 64}]},
            )


@pytest.mark.asyncio
async def test_pending_not_visible_in_ls(
    client, user_token, datanode_headers, registered_datanodes
):
    """OB1: objeto en PENDING no aparece en ls."""
    m = _manifest()
    r = await client.post("/objects/init", headers=_auth(user_token), json=m)
    assert r.status_code == 201
    # Sin commit todavia.
    r_ls = await client.get("/ns/ls?path=/", headers=_auth(user_token))
    names = [e["name"] for e in r_ls.json()["entries"]]
    assert "f.bin" not in names


@pytest.mark.asyncio
async def test_pending_not_readable(
    client, user_token, datanode_headers, registered_datanodes
):
    """OB2: GET /objects de un PENDING => 404."""
    m = _manifest()
    await client.post("/objects/init", headers=_auth(user_token), json=m)
    r = await client.get(
        "/objects?key=/f.bin", headers=_auth(user_token)
    )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_commit_incomplete_replicas_rejected(
    client, user_token, datanode_headers, registered_datanodes
):
    """OB3: commit sin todas las replicas confirmadas => 409."""
    m = _manifest()
    r_init = await client.post("/objects/init", headers=_auth(user_token), json=m)
    object_id = r_init.json()["object_id"]

    # Confirmar solo 2 de 3 replicas del primer bloque.
    block_id = m["blocks"][0]["block_id"]
    for node in ["dn-1", "dn-2"]:
        await client.post(
            f"/datanodes/{node}/blocks",
            headers=datanode_headers,
            json={"blocks": [{"block_id": block_id, "checksum": "a"}]},
        )

    r = await client.post(
        f"/objects/{object_id}/commit", headers=_auth(user_token)
    )
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert "missing" in (detail if isinstance(detail, dict) else str(detail))


@pytest.mark.asyncio
async def test_full_put_commit_lifecycle(
    client, user_token, datanode_headers, registered_datanodes
):
    """OB4: full lifecycle: init + confirmar todas + commit => COMMITTED, visible."""
    m = _manifest(n_blocks=1)
    r_init = await client.post("/objects/init", headers=_auth(user_token), json=m)
    assert r_init.status_code == 201
    object_id = r_init.json()["object_id"]

    block_ids = [b["block_id"] for b in m["blocks"]]
    await _confirm_all_replicas(client, datanode_headers, object_id, block_ids)

    r_commit = await client.post(
        f"/objects/{object_id}/commit", headers=_auth(user_token)
    )
    assert r_commit.status_code == 200
    body = r_commit.json()
    assert body["status"] == "COMMITTED"
    assert body["version"] == 1

    # Aparece en ls.
    r_ls = await client.get("/ns/ls?path=/", headers=_auth(user_token))
    names = [e["name"] for e in r_ls.json()["entries"]]
    assert "f.bin" in names

    # Es leible por clave.
    r_get = await client.get("/objects?key=/f.bin", headers=_auth(user_token))
    assert r_get.status_code == 200
    got = r_get.json()
    assert got["object_id"] == object_id
    assert len(got["blocks"]) == 1
    assert len(got["blocks"][0]["replicas"]) == 3
    # Verificar que las replicas traen node_id y address, NUNCA contenido.
    for repl in got["blocks"][0]["replicas"]:
        assert "node_id" in repl
        assert "address" in repl
        # No hay campo de datos.
        assert "data" not in repl
        assert "bytes" not in repl


@pytest.mark.asyncio
async def test_put_same_key_twice_creates_two_versions(
    client, user_token, datanode_headers, registered_datanodes
):
    """OB5: dos init+commit del mismo key => dos versiones COMMITTED."""
    # Primer put.
    m1 = _manifest(object_key="/v.bin", n_blocks=1)
    r1 = await client.post("/objects/init", headers=_auth(user_token), json=m1)
    obj1 = r1.json()["object_id"]
    await _confirm_all_replicas(
        client, datanode_headers, obj1, [m1["blocks"][0]["block_id"]]
    )
    r_c1 = await client.post(f"/objects/{obj1}/commit", headers=_auth(user_token))
    assert r_c1.json()["version"] == 1

    # Segundo put del mismo key.
    m2 = _manifest(object_key="/v.bin", n_blocks=1)
    r2 = await client.post("/objects/init", headers=_auth(user_token), json=m2)
    obj2 = r2.json()["object_id"]
    await _confirm_all_replicas(
        client, datanode_headers, obj2, [m2["blocks"][0]["block_id"]]
    )
    r_c2 = await client.post(f"/objects/{obj2}/commit", headers=_auth(user_token))
    assert r_c2.json()["version"] == 2

    # stat muestra las 2 versiones.
    r_stat = await client.get(
        f"/objects/{obj2}/stat", headers=_auth(user_token)
    )
    versions = r_stat.json()["versions"]
    assert len(versions) == 2
    # ls muestra 1 entrada (ultima version).
    r_ls = await client.get("/ns/ls?path=/", headers=_auth(user_token))
    names = [e["name"] for e in r_ls.json()["entries"]]
    assert names.count("v.bin") == 1


@pytest.mark.asyncio
async def test_delete_hides_from_ls(
    client, user_token, datanode_headers, registered_datanodes
):
    """OB7: commit + delete => ls no muestra, stat muestra DELETED."""
    m = _manifest(object_key="/d.bin", n_blocks=1)
    r = await client.post("/objects/init", headers=_auth(user_token), json=m)
    object_id = r.json()["object_id"]
    await _confirm_all_replicas(
        client, datanode_headers, object_id, [m["blocks"][0]["block_id"]]
    )
    await client.post(f"/objects/{object_id}/commit", headers=_auth(user_token))

    r_del = await client.request(
        "DELETE", f"/objects/{object_id}", headers=_auth(user_token)
    )
    assert r_del.status_code == 204

    r_ls = await client.get("/ns/ls?path=/", headers=_auth(user_token))
    names = [e["name"] for e in r_ls.json()["entries"]]
    assert "d.bin" not in names

    r_stat = await client.get(
        f"/objects/{object_id}/stat", headers=_auth(user_token)
    )
    versions = r_stat.json()["versions"]
    assert versions[0]["status"] == "DELETED"


@pytest.mark.asyncio
async def test_user_isolation(
    client, user_token, second_user_token, datanode_headers, registered_datanodes
):
    """OB8: usuario A commit => usuario B no lo ve ni por ls ni por GET."""
    m = _manifest(object_key="/priv.bin", n_blocks=1)
    r = await client.post("/objects/init", headers=_auth(user_token), json=m)
    object_id = r.json()["object_id"]
    await _confirm_all_replicas(
        client, datanode_headers, object_id, [m["blocks"][0]["block_id"]]
    )
    await client.post(f"/objects/{object_id}/commit", headers=_auth(user_token))

    r_ls = await client.get("/ns/ls?path=/", headers=_auth(second_user_token))
    names = [e["name"] for e in r_ls.json()["entries"]]
    assert "priv.bin" not in names

    r_get = await client.get(
        "/objects?key=/priv.bin", headers=_auth(second_user_token)
    )
    assert r_get.status_code == 404

    # Y no puede borrarlo.
    r_del = await client.request(
        "DELETE", f"/objects/{object_id}", headers=_auth(second_user_token)
    )
    assert r_del.status_code == 404


@pytest.mark.asyncio
async def test_insufficient_datanodes_returns_507(client, user_token):
    """OB9: sin DataNodes registrados => init retorna 507."""
    m = _manifest()
    r = await client.post("/objects/init", headers=_auth(user_token), json=m)
    assert r.status_code == 507


@pytest.mark.asyncio
async def test_commit_wrong_state_already_committed(
    client, user_token, datanode_headers, registered_datanodes
):
    """OB10: commitear un objeto ya COMMITTED => 422."""
    m = _manifest(object_key="/dc.bin", n_blocks=1)
    r = await client.post("/objects/init", headers=_auth(user_token), json=m)
    object_id = r.json()["object_id"]
    await _confirm_all_replicas(
        client, datanode_headers, object_id, [m["blocks"][0]["block_id"]]
    )
    await client.post(f"/objects/{object_id}/commit", headers=_auth(user_token))
    r2 = await client.post(f"/objects/{object_id}/commit", headers=_auth(user_token))
    assert r2.status_code == 422
