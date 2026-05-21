#!/usr/bin/env python3
"""Test WS: start pipeline, print events for 10s."""
import asyncio, json, time
import websockets

async def test():
    async with websockets.connect("ws://127.0.0.1:9876") as ws:
        print("[>] start_pipeline")
        await ws.send(json.dumps({
            "type": "start_pipeline",
            "model_dir": "/home/soloopooo/gitworkdir/whiscap/backend-sherpa/models/zh-en",
            "language": "zh",
            "audio_backend": "pipewire",
        }))
        deadline = time.time() + 10
        events = []
        while time.time() < deadline:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=2)
                d = json.loads(msg)
                events.append(d)
                print(f"[<] type={d.get('type')} is_final={d.get('is_final')} text={str(d.get('text',''))[:80]}")
            except asyncio.TimeoutError:
                print("[.] (no message in 2s)")
                continue
        print(f"\n=== Total events: {len(events)} ===")
        await ws.close()

asyncio.run(test())
