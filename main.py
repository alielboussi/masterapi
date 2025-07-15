from fastapi import FastAPI, Request
from typing import List, Dict, Any
from datetime import datetime

app = FastAPI()
storage: List[Dict[str, Any]] = []

@app.post("/api/pushsale")
async def push_sale(request: Request):
    data = await request.json()
    data['received_at'] = datetime.utcnow().isoformat()
    storage.append(data)
    return {"status": "success"}

@app.get("/api/allsales")
def get_all_sales():
    return storage
