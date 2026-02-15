import asyncio
import subprocess
import logging
import os
import signal
import sys
from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import threading

# --- Application Setup ---
app = FastAPI(title="Heavy Aggregator Controller")

# CORS for React Frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allow all for local dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Logging Manager ---
class LogManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.loop = None # Captured on startup

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                pass
    
    def broadcast_threadsafe(self, message: str):
        if self.loop:
            asyncio.run_coroutine_threadsafe(self.broadcast(message), self.loop)

log_manager = LogManager()

# --- ScraperManager ---
class ScraperManager:
    def __init__(self):
        self.process = None
        self.status = "IDLE"
        self.lock = threading.Lock()

    def start_scraper(self, site: str):
        with self.lock:
            if self.process and self.process.poll() is None:
                raise HTTPException(status_code=400, detail="Scraper is already running.")
            
            self.status = "RUNNING"
            cmd = [sys.executable, "main.py", "--site", site, "--output-format", "json"]
            
            # Using Popen with text=True and line buffering
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=os.getcwd()
            )
            
            threading.Thread(target=self._monitor_output, args=(self.process,), daemon=True).start()

    def stop_scraper(self):
        with self.lock:
            if self.process and self.process.poll() is None:
                self.process.terminate()
                self.status = "IDLE"
                log_manager.broadcast_threadsafe("--- Scraper Stopped by User ---\n")

    def _monitor_output(self, process):
        try:
            for line in iter(process.stdout.readline, ''):
                if line:
                    log_manager.broadcast_threadsafe(line)
        except Exception as e:
            log_manager.broadcast_threadsafe(f"Error reading output: {e}\n")
        finally:
             if process.stdout: process.stdout.close()
             process.wait()
             self.status = "IDLE"
             log_manager.broadcast_threadsafe("--- Scraper Finished ---\n")

scraper_manager = ScraperManager()

# --- Startup ---
@app.on_event("startup")
async def startup_event():
    log_manager.loop = asyncio.get_running_loop()

# --- Models ---
class ScrapeRequest(BaseModel):
    site: str

# --- Routes ---

@app.get("/api/status")
async def get_status():
    return {"status": scraper_manager.status}

@app.post("/api/scrape")
async def start_scrape(request: ScrapeRequest):
    ValidSites = ['nasga', 'heavyathlete', 'scottishscores', 'all']
    if request.site not in ValidSites:
         raise HTTPException(status_code=400, detail=f"Invalid site. Must be one of {ValidSites}")
         
    scraper_manager.start_scraper(request.site)
    return {"status": "started", "site": request.site}

@app.post("/api/stop")
async def stop_scrape():
    scraper_manager.stop_scraper()
    return {"status": "stopped"}

@app.websocket("/api/logs")
async def websocket_endpoint(websocket: WebSocket):
    await log_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text() # Keep connection open, ignore input
    except WebSocketDisconnect:
        log_manager.disconnect(websocket)

# --- File Explorer Routes ---
@app.get("/api/files")
def list_files(path: str = ""):
    """Lists files in the output directory."""
    base_path = "output"
    target_path = os.path.join(base_path, path)
    
    # Security check to prevent .. traversal
    if not os.path.abspath(target_path).startswith(os.path.abspath(base_path)):
        raise HTTPException(status_code=403, detail="Access denied")
        
    if not os.path.exists(target_path):
         return []

    items = []
    for entry in os.scandir(target_path):
        items.append({
            "name": entry.name,
            "is_dir": entry.is_dir(),
            "path": os.path.relpath(entry.path, base_path)
        })
    
    # Sort: Directories first, then files
    items.sort(key=lambda x: (not x['is_dir'], x['name']))
    return items

@app.get("/api/files/content")
def get_file_content(path: str):
    """Returns content of a file."""
    base_path = "output"
    target_path = os.path.join(base_path, path)
    
    if not os.path.abspath(target_path).startswith(os.path.abspath(base_path)):
         raise HTTPException(status_code=403, detail="Access denied")
    
    if not os.path.exists(target_path) or not os.path.isfile(target_path):
         raise HTTPException(status_code=404, detail="File not found")

    try:
        with open(target_path, 'r') as f:
            return f.read()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
