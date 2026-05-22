import asyncio
import json
import os
import re

from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import websockets

STREAMERBOT_WS = os.getenv("STREAMERBOT_WS", "ws://localhost:8000")

app = FastAPI()

# =========================
# CORS (IMPORTANT FOR OBS/LOCAL TESTING)
# =========================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# STATIC FILES (THIS FIXES YOUR /overlay.html ISSUE)
# =========================

app.mount("/", StaticFiles(directory="static", html=True), name="static")

# =========================
# WEBSOCKET CLIENTS
# =========================

clients = set()
queue = asyncio.Queue()

# =========================
# BROADCAST SYSTEM
# =========================

async def broadcaster():
    while True:
        msg = await queue.get()

        dead = []
        for ws in clients:
            try:
                await ws.send_json(msg)
            except:
                dead.append(ws)

        for d in dead:
            clients.remove(d)

# =========================
# NORMALIZE MESSAGE
# =========================

async def push(platform, user, message, badges=None):
    await queue.put({
        "platform": platform,
        "user": user,
        "message": message,
        "badges": badges or []
    })

# =========================
# PARSE TIKTOK / RUMBLE TEXT
# (platform) username: message
# =========================

def parse_text(text: str):
    match = re.match(r"\((.*?)\)\s*(.*?):\s*(.*)", text)
    if not match:
        return None

    return (
        match.group(1).lower(),
        match.group(2),
        match.group(3)
    )

# =========================
# STREAMER.BOT WORKER
# =========================

from fastapi import Request

@app.post("/event")
async def event(req: Request):
    data = await req.json()
    await queue.put(data)
    return {"ok": True}

# =========================
# STARTUP
# =========================

@app.on_event("startup")
async def startup():
    asyncio.create_task(broadcaster())
    asyncio.create_task(streamerbot_worker())

# =========================
# WEBSOCKET ENDPOINT
# =========================

@app.websocket("/chat")
async def chat(ws: WebSocket):
    await ws.accept()
    clients.add(ws)

    try:
        while True:
            await ws.receive_text()
    except:
        clients.remove(ws)
