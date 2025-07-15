from fastapi import FastAPI, Request
import sqlite3
from typing import Dict, Any

app = FastAPI()

# --- Database helpers ---
def get_conn():
    conn = sqlite3.connect("masterdata.db")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cursor = conn.cursor()
    # 1. Closing
    cursor.execute('''CREATE TABLE IF NOT EXISTS Closing (
        id INTEGER,
        itemid INTEGER,
        date TEXT,
        quantity REAL,
        Discard REAL,
        Remaining REAL,
        staff TEXT,
        completewaste REAL,
        uploadstatus INTEGER,
        remarks TEXT,
        Onlineid INTEGER,
        branchid INTEGER,
        Userid INTEGER,
        kdsid INTEGER,
        received_at TEXT
    )''')
    # 2. DayEnd
    cursor.execute('''CREATE TABLE IF NOT EXISTS DayEnd (
        Id INTEGER,
        Date TEXT,
        DayStatus TEXT,
        UserId INTEGER,
        uploadstatus INTEGER,
        branchid INTEGER,
        DayStartTime TEXT,
        DayEndTime TEXT,
        received_at TEXT
    )''')
    # 3. InventoryConsumed
    cursor.execute('''CREATE TABLE IF NOT EXISTS InventoryConsumed (
        Id INTEGER,
        RawItemId INTEGER,
        QuantityConsumed REAL,
        RemainingQuantity REAL,
        Date TEXT,
        branchid INTEGER,
        uploadstatus INTEGER,
        kdsid INTEGER,
        typec TEXT,
        received_at TEXT
    )''')
    # 4. IssueStock
    cursor.execute('''CREATE TABLE IF NOT EXISTS IssueStock (
        Id INTEGER,
        IssueBranchId INTEGER,
        issuingStore TEXT,
        ReceiveBranchId INTEGER,
        ReceivingStore TEXT,
        TotalAmount REAL,
        Date TEXT,
        BranchCode TEXT,
        StoreCode TEXT,
        InvoiceNo TEXT,
        Status TEXT,
        UploadStatus INTEGER,
        branchid INTEGER,
        received_at TEXT
    )''')
    # 5. Sale
    cursor.execute('''CREATE TABLE IF NOT EXISTS Sale (
        Id INTEGER,
        Date TEXT,
        time TEXT,
        UserId INTEGER,
        TotalBill REAL,
        Discount REAL,
        DiscountAmount REAL,
        NetBill REAL,
        BillType TEXT,
        OrderType TEXT,
        GST REAL,
        BillStatus TEXT,
        OrderStatus TEXT,
        Terminal TEXT,
        UploadStatus INTEGER,
        branchid INTEGER,
        GSTPerc REAL,
        Shiftid INTEGER,
        Customer TEXT,
        message TEXT,
        Deliverystatus TEXT,
        servicecharges REAL,
        TerminalOrder INTEGER,
        invoice TEXT,
        received_at TEXT
    )''')
    # 6. Saledetails
    cursor.execute('''CREATE TABLE IF NOT EXISTS Saledetails (
        Id INTEGER,
        saleid INTEGER,
        MenuItemId INTEGER,
        Flavourid INTEGER,
        ModifierId INTEGER,
        RuntimeModifierId INTEGER,
        Quantity REAL,
        Price REAL,
        BarnchCode TEXT,
        Status INTEGER,
        comments TEXT,
        Orderstatus TEXT,
        branchid INTEGER,
        Itemdiscount REAL,
        uploadstatus INTEGER,
        received_at TEXT
    )''')
    conn.commit()
    conn.close()
init_db()

# --- Helper: insert row to any table ---
def insert_to_table(table: str, data: Dict[str, Any]):
    conn = get_conn()
    cursor = conn.cursor()
    columns = ','.join(data.keys())
    placeholders = ','.join('?' for _ in data)
    sql = f'INSERT INTO {table} ({columns},received_at) VALUES ({placeholders},datetime("now"))'
    values = list(data.values())
    cursor.execute(sql, values)
    conn.commit()
    conn.close()

# --- POST endpoints for each table ---
@app.post("/api/closing")
async def push_closing(request: Request):
    data = await request.json()
    insert_to_table("Closing", data)
    return {"status": "success"}

@app.post("/api/dayend")
async def push_dayend(request: Request):
    data = await request.json()
    insert_to_table("DayEnd", data)
    return {"status": "success"}

@app.post("/api/inventoryconsumed")
async def push_inventoryconsumed(request: Request):
    data = await request.json()
    insert_to_table("InventoryConsumed", data)
    return {"status": "success"}

@app.post("/api/issuestock")
async def push_issuestock(request: Request):
    data = await request.json()
    insert_to_table("IssueStock", data)
    return {"status": "success"}

@app.post("/api/sale")
async def push_sale(request: Request):
    data = await request.json()
    insert_to_table("Sale", data)
    return {"status": "success"}

@app.post("/api/saledetails")
async def push_saledetails(request: Request):
    data = await request.json()
    insert_to_table("Saledetails", data)
    return {"status": "success"}

# --- GET endpoint: all data, grouped by branchid ---
@app.get("/api/alldata")
def get_all_data():
    conn = get_conn()
    cursor = conn.cursor()
    tables = ["Closing", "DayEnd", "InventoryConsumed", "IssueStock", "Sale", "Saledetails"]
    result = {}
    for table in tables:
        cursor.execute(f"SELECT * FROM {table}")
        rows = [dict(row) for row in cursor.fetchall()]
        # group by branchid:
        for row in rows:
            branchid = row.get("branchid")
            if branchid is None:
                continue
            if branchid not in result:
                result[branchid] = {t: [] for t in tables}
            result[branchid][table].append(row)
    conn.close()
    return result
