import asyncio
import json
import os
import re

from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import websockets

STREAMERBOT_WS = os.getenv("STREAMERBOT_WS", "ws://localhost:8080")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
# NORMALIZE OUTPUT
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
# FORMAT: (platform) user: message
# =========================

def parse_text(text: str):
    match = re.match(r"\((.*?)\)\s*(.*?):\s*(.*)", text)
    if not match:
        return None

    return (
        match.group(1).lower().strip(),
        match.group(2).strip(),
        match.group(3).strip()
    )

# =========================
# STREAMER.BOT WORKER
# =========================

async def streamerbot_worker():

    async with websockets.connect(STREAMERBOT_WS) as ws:

        while True:
            raw = await ws.recv()

            try:
                msg = json.loads(raw)

                # =========================
                # TIKTOK / RUMBLE (TEXT MODE)
                # =========================

                if "websocketClient" in msg:
                    event_type = msg["websocketClient"]

                    if event_type == "Open":
                        print("Connected to Streamer.bot")

                    elif event_type == "Close":
                        print("Disconnected from Streamer.bot")

                    elif event_type == "Message":
                        text = msg.get("data", "")

                        parsed = parse_text(text)

                        if parsed:
                            platform, user, message = parsed

                            if platform in ["tiktok", "rumble"]:
                                await push(platform, user, message)

                    continue

                # =========================
                # TWITCH / YOUTUBE / KICK (JSON MODE)
                # =========================

                event = msg.get("event", {})
                data = msg.get("data", {})

                source = event.get("source")
                etype = event.get("type")

                if etype != "chat_message":
                    continue

                if source not in ["twitch", "youtube", "kick"]:
                    continue

                await push(
                    source,
                    data.get("user", "unknown"),
                    data.get("message", ""),
                    data.get("badges", [])
                )

            except:
                pass

# =========================
# STARTUP
# =========================

@app.on_event("startup")
async def startup():
    asyncio.create_task(broadcaster())
    asyncio.create_task(streamerbot_worker())

# =========================
# OBS WEBSOCKET
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
