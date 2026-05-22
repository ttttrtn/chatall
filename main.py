import asyncio
import re

from fastapi import FastAPI, WebSocket, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# =========================
# APP SETUP
# =========================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve overlay + assets safely
app.mount("/static", StaticFiles(directory="static"), name="static")

# =========================
# STATE
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

    return {
        "platform": match.group(1).lower(),
        "user": match.group(2).strip(),
        "message": match.group(3).strip(),
        "badges": []
    }

# =========================
# STREAMER.BOT / BRIDGE INPUT
# =========================

@app.post("/event")
async def event(req: Request):
    data = await req.json()

    # If it's raw text format from TikTok/Rumble
    if isinstance(data, str):
        parsed = parse_text(data)
        if parsed:
            await push(**parsed)
            return {"ok": True}

    # If it's structured event
    await queue.put(data)
    return {"ok": True}

# =========================
# WEBSOCKET FOR OVERLAY
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

# =========================
# STARTUP TASKS
# =========================

@app.on_event("startup")
async def startup():
    asyncio.create_task(broadcaster())
