# -*- coding: utf-8 -*-
"""iqless Google One activation API client."""

import httpx

from config import IQLESS_API_KEY, IQLESS_BASE_URL, logger


async def iqless_pick_best_device() -> tuple:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{IQLESS_BASE_URL}/api/health")
        h = resp.json()
        devices = h.get("pools", {}).get("unified", {}).get("devices", [])
        if not devices:
            return None, "no_devices"
        busy_devices = [d for d in devices if d.get("connected") and d.get("busy")]
        ready_devices = [d for d in devices if d.get("connected") and not d.get("busy")]
        if busy_devices:
            return busy_devices[0]["serial"], "busy"
        if ready_devices:
            return ready_devices[0]["serial"], "ready"
        return None, "all_unavailable"
    except Exception:
        return None, "health_error"


async def iqless_submit_job(email: str, password: str, totp_secret: str, device: str = None) -> dict:
    headers = {"X-API-Key": IQLESS_API_KEY, "Content-Type": "application/json"}
    payload = {"email": email, "password": password, "totp_secret": totp_secret}
    if device:
        payload["device"] = device
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{IQLESS_BASE_URL}/api/jobs", headers=headers, json=payload)
        try:
            body = resp.json()
        except Exception:
            body = {"detail": {"code": "PARSE_ERROR", "message": resp.text}}
        return resp.status_code, body
    except httpx.TimeoutException:
        return 504, {"detail": {"code": "TIMEOUT", "message": "Request timed out"}}
    except Exception as e:
        return 503, {"detail": {"code": "NETWORK_ERROR", "message": str(e)}}


async def iqless_poll_job(job_id: str) -> dict:
    headers = {"X-API-Key": IQLESS_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{IQLESS_BASE_URL}/api/jobs/{job_id}", headers=headers)
        try:
            data = resp.json()
        except Exception:
            data = {}
        if resp.status_code != 200:
            detail = data.get("detail", {})
            if isinstance(detail, str):
                detail = {"code": "api_error", "message": detail}
            return {"status": "error", "error": detail.get("code", "HTTP_" + str(resp.status_code)), "detail": detail}
        return data
    except httpx.TimeoutException:
        return {"status": "error", "error": "TIMEOUT", "detail": {"message": "Request timed out"}}
    except Exception as e:
        return {"status": "error", "error": "NETWORK_ERROR", "detail": {"message": str(e)}}


async def iqless_get_balance() -> dict:
    headers = {"X-API-Key": IQLESS_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{IQLESS_BASE_URL}/api/balance", headers=headers)
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


async def iqless_get_queue() -> dict:
    headers = {"X-API-Key": IQLESS_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{IQLESS_BASE_URL}/api/queue", headers=headers)
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


async def iqless_cancel_job(job_id: str) -> tuple:
    headers = {"X-API-Key": IQLESS_API_KEY, "Content-Type": "application/json"}
    attempts = [
        ("POST", f"{IQLESS_BASE_URL}/api/jobs/{job_id}/cancel"),
        ("DELETE", f"{IQLESS_BASE_URL}/api/queue/{job_id}"),
        ("POST", f"{IQLESS_BASE_URL}/api/queue/remove"),
    ]
    for method, url in attempts:
        async with httpx.AsyncClient(timeout=15) as client:
            kwargs = {"headers": headers}
            if method == "POST" and "remove" in url:
                kwargs["json"] = {"job_id": job_id}
            resp = await client.request(method, url, **kwargs)
        if resp.status_code not in (404, 405):
            try:
                body = resp.json()
            except Exception:
                body = {"message": resp.text}
            if not isinstance(body, dict):
                body = {"message": str(body)}
            return resp.status_code, body
    return 404, {"detail": {"code": "NO_CANCEL_ENDPOINT", "message": "This API does not support cancellations yet"}}
