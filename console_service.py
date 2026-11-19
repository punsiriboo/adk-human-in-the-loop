"""Console service: the support console for supervisors and human agents.

Run:   uvicorn console_service:app --port 8100
Open:  http://localhost:8100/          support console

This service holds no conversation state. It serves the console page and relays
its /api/console/* calls to the chat service (chat_service.py):

  browser --GET  /api/console/stream--> here --SSE  /internal/stream--> chat service
  browser --POST /api/console/...-----> here --HTTP /internal/...-----> chat service

Settings (env or support_agent/.env):
  CHAT_SERVICE_URL   where the chat service runs (default http://localhost:8000)
  INTERNAL_TOKEN     shared secret, must match the chat service's

On Cloud Run, put this service behind IAP (or other staff sign-in) and keep the
chat service's /internal/* reachable only from here.
"""

import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

load_dotenv(Path(__file__).parent / "support_agent" / ".env")

CHAT_SERVICE_URL = os.getenv("CHAT_SERVICE_URL", "http://localhost:8000").rstrip("/")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN", "")
STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    headers = {"X-Internal-Token": INTERNAL_TOKEN} if INTERNAL_TOKEN else {}
    async with httpx.AsyncClient(base_url=f"{CHAT_SERVICE_URL}/internal", headers=headers,
                                 timeout=httpx.Timeout(15, read=None)) as client:
        app.state.chat = client
        yield


app = FastAPI(title="Shopio console service", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


def unavailable(exc: Exception) -> JSONResponse:
    return JSONResponse({"detail": f"Chat service unavailable: {exc}"}, status_code=502)


@app.get("/api/console/stream")
async def stream(request: Request) -> Response:
    """Relay the chat service's event stream to this browser.

    One upstream connection per open console. If the chat service drops, the
    stream ends and the browser's EventSource reconnects and reloads its data.
    """
    client: httpx.AsyncClient = request.app.state.chat
    try:
        upstream = await client.send(client.build_request("GET", "/stream"), stream=True)
    except httpx.HTTPError as exc:
        return unavailable(exc)
    if upstream.status_code != 200:
        await upstream.aclose()
        return JSONResponse({"detail": "Chat service refused the stream"}, status_code=502)

    async def relay():
        try:
            async for chunk in upstream.aiter_raw():
                yield chunk
        except httpx.HTTPError:
            pass  # upstream went away; ending the stream makes the browser reconnect
        finally:
            await upstream.aclose()

    return StreamingResponse(relay(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.api_route("/api/console/{path:path}", methods=["GET", "POST"])
async def relay_call(path: str, request: Request) -> Response:
    """Forward a console call (snapshot, approve, join, reply, release, typing)."""
    client: httpx.AsyncClient = request.app.state.chat
    try:
        r = await client.request(request.method, f"/{path}", content=await request.body(),
                                 headers={"content-type": request.headers.get("content-type", "application/json")})
    except httpx.HTTPError as exc:
        return unavailable(exc)
    return Response(r.content, status_code=r.status_code, media_type=r.headers.get("content-type"))


@app.get("/")
def console_page() -> FileResponse:
    return FileResponse(STATIC / "console.html")
