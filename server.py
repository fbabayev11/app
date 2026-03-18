from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx, json, os, re, sqlite3
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── SQLite DB ──────────────────────────────────────────────────────────────────
DATA_DIR = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", ".")
DB_PATH  = os.path.join(DATA_DIR, "packing.db")

def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db

def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS archive (
            id INTEGER PRIMARY KEY,
            order_id INTEGER,
            order_name TEXT,
            archived_at TEXT
        );
        CREATE TABLE IF NOT EXISTS product_images (
            name TEXT PRIMARY KEY,
            data TEXT
        );
    """)
    db.commit(); db.close()

init_db()

def load_config():
    db = get_db()
    rows = db.execute("SELECT key, value FROM config").fetchall()
    db.close()
    return {r["key"]: r["value"] for r in rows}

def save_config(data: dict):
    db = get_db()
    for k, v in data.items():
        db.execute("INSERT OR REPLACE INTO config(key,value) VALUES(?,?)", (k, v))
    db.commit(); db.close()

def load_archive():
    db = get_db()
    rows = db.execute("SELECT order_id as id, order_name as name, archived_at FROM archive ORDER BY rowid DESC").fetchall()
    db.close()
    return [dict(r) for r in rows]

def save_archive_item(order_id: int, order_name: str):
    db = get_db()
    db.execute("INSERT OR REPLACE INTO archive(order_id,order_name,archived_at) VALUES(?,?,?)",
               (order_id, order_name, datetime.now().strftime("%Y-%m-%d %H:%M")))
    db.commit(); db.close()

def delete_archive_item(order_id: int):
    db = get_db()
    db.execute("DELETE FROM archive WHERE order_id=?", (order_id,))
    db.commit(); db.close()

# ── Shopify helpers ────────────────────────────────────────────────────────────

async def shopify_get(shop, token, endpoint, params={}):
    url = f"https://{shop}/admin/api/2024-01/{endpoint}"
    headers = {"X-Shopify-Access-Token": token}
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(url, params=params, headers=headers)
        if r.status_code != 200:
            raise Exception(f"Shopify {r.status_code}: {r.text[:200]}")
        return r.json(), r.headers

async def fetch_all_orders(shop, token):
    """Fulfillment gözləyən sifarişləri çək"""
    all_orders = []
    page_info = None
    first = True
    while True:
        if first:
            # Son 60 günün sifarişləri
            from datetime import timezone
            since = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
            params = {
                "status": "open",
                "fulfillment_status": "unfulfilled",
                "created_at_min": since,
                "limit": 250,
                "fields": "id,name,created_at,line_items,shipping_address,note,tags,financial_status,line_items.variant_id,line_items.product_id"
            }
        else:
            params = {"limit": 250, "page_info": page_info,
                      "fields": "id,name,created_at,line_items,shipping_address,note,tags,financial_status,line_items.variant_id,line_items.product_id"}

        data, headers = await shopify_get(shop, token, "orders.json", params)
        orders = data.get("orders", [])
        all_orders.extend(orders)

        link = headers.get("Link","")
        next_pi = None
        if 'rel="next"' in link:
            m = re.search(r'page_info=([^&>]+)[^>]*>;\s*rel="next"', link)
            if m: next_pi = m.group(1)
        if not next_pi or not orders: break
        page_info = next_pi
        first = False

    return all_orders

def get_prop(li, key):
    for p in li.get("properties", []):
        if str(p.get("name","")).strip() == key:
            return p.get("value")
    return None

def transform_order(o):
    raw_items = o.get("line_items", [])

    # Group ID integer ola bilər - str-ə çevir
    custom_boxes = {}   # group_id(str) → [sub-items]
    normal_items = []
    parent_items = {}   # group_id(str) → parent line item

    for li in raw_items:
        # Ana qutu: _has_gpo + _gpo_product_group var
        has_gpo = get_prop(li, "_has_gpo")
        grp_id  = get_prop(li, "_gpo_product_group")
        parent_grp = get_prop(li, "_gpo_parent_product_group")

        if has_gpo is not None and grp_id is not None:
            # Bu ana məhsuldur (Customized Sweet Box)
            key = str(grp_id)
            parent_items[key] = li
        elif parent_grp is not None:
            # Bu qutunun içindəki sub-məhsuldur
            key = str(parent_grp)
            if key not in custom_boxes:
                custom_boxes[key] = []
            custom_boxes[key].append(li)
        else:
            normal_items.append(li)

    items = []

    # Normal məhsullar
    for li in normal_items:
        items.append({
            "name": li.get("name",""),
            "quantity": li.get("quantity", 1),
            "sku": li.get("sku",""),
            "variant": li.get("variant_title","") or "",
            "is_custom_box": False,
            "box_contents": [],
            "variant_id": li.get("variant_id"),
            "product_id": li.get("product_id"),
        })

    # Customized box-ları birləşdir
    for group_id, sub_items in custom_boxes.items():
        parent   = parent_items.get(group_id)
        box_name = parent.get("name","Customized Box") if parent else "Customized Box"
        box_qty  = parent.get("quantity", 1) if parent else 1

        contents = []
        for si in sub_items:
            # Ad: "Customized Box - Paxlava" → "Paxlava"
            raw_name = si.get("name","")
            clean    = raw_name.replace("Customized Box - ","").replace("Customized Box – ","").strip()
            # Say: _gpo_quantity_mix property-dən
            qty_mix  = get_prop(si, "_gpo_quantity_mix")
            qty      = int(qty_mix) if qty_mix is not None else si.get("quantity", 1)
            contents.append({
                "name": clean,
                "quantity": qty,
                "variant_id": si.get("variant_id"),
                "product_id": si.get("product_id"),
            })

        items.append({
            "name": box_name,
            "quantity": box_qty,
            "sku": "",
            "variant": "",
            "is_custom_box": True,
            "box_contents": contents,
        })
    addr = o.get("shipping_address") or {}
    return {
        "id": o["id"],
        "name": o.get("name",""),
        "created_at": o.get("created_at","")[:10],
        "items": items,
        "total_items": sum(
            sum(c["quantity"] for c in i["box_contents"]) if i.get("is_custom_box") and i.get("box_contents")
            else i["quantity"]
            for i in items
        ),
        "country": addr.get("country",""),
        "country_code": addr.get("country_code",""),
        "city": addr.get("city",""),
        "customer": f"{addr.get('first_name','')} {addr.get('last_name','')}".strip(),
        "financial_status": o.get("financial_status",""),
        "note": o.get("note","") or "",
        "tags": o.get("tags","") or "",
    }

# ── Lokal şəkil sistemi ───────────────────────────────────────────────────────
def load_images():
    db = get_db()
    rows = db.execute("SELECT name, data FROM product_images").fetchall()
    db.close()
    return {r["name"]: r["data"] for r in rows}

def save_image(name: str, data: str):
    db = get_db()
    db.execute("INSERT OR REPLACE INTO product_images(name,data) VALUES(?,?)", (name, data))
    db.commit(); db.close()

def delete_image_db(name: str):
    db = get_db()
    db.execute("DELETE FROM product_images WHERE name=?", (name,))
    db.commit(); db.close()

def match_image(name: str, images: dict) -> str:
    """Yalnız exact key uyğunluğu — fuzzy yox"""
    if not name or not images: return ""
    return images.get(name, "")

# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/product-images")
def get_product_images():
    return load_images()

@app.post("/product-images")
async def upload_product_image(request: Request):
    body = await request.json()
    name = body.get("name","").strip()
    data = body.get("data","")
    if not name or not data:
        raise HTTPException(400, "name və data lazımdır")
    save_image(name, data)
    return {"ok": True}

@app.delete("/product-images")
def delete_product_image(name: str):
    delete_image_db(name)
    return {"ok": True}

@app.get("/all-orders")
async def get_all_orders():
    cfg = load_config()
    if not cfg.get("shop"):
        raise HTTPException(400, "Konfiqurasiya edilməyib")
    
    all_orders = []
    page_info = None
    first = True
    
    while True:
        if first:
            from datetime import timezone
            since = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
            params = {
                "status": "any",
                "created_at_min": since,
                "limit": 250,
                "fields": "id,name,created_at,line_items,shipping_address,financial_status,fulfillment_status,total_price,fulfillments,source_name"
            }
        else:
            params = {"limit": 250, "page_info": page_info,
                      "fields": "id,name,created_at,line_items,shipping_address,financial_status,fulfillment_status,total_price,fulfillments,source_name"}
        
        data, headers = await shopify_get(cfg["shop"], cfg["token"], "orders.json", params)
        orders = data.get("orders", [])
        all_orders.extend(orders)
        
        link = headers.get("Link","")
        next_pi = None
        if 'rel="next"' in link:
            m = re.search(r'page_info=([^&>]+)[^>]*>;\s*rel="next"', link)
            if m: next_pi = m.group(1)
        if not next_pi or not orders: break
        page_info = next_pi
        first = False
    
    result = []
    for o in all_orders:
        # İzləmə nömrələrini topla
        tracking = []
        for f in o.get("fulfillments", []):
            tn = f.get("tracking_number")
            tc = f.get("tracking_company","")
            tu = f.get("tracking_url","")
            if tn:
                tracking.append({"number": tn, "company": tc, "url": tu})
        
        addr = o.get("shipping_address") or {}
        customer = f"{addr.get('first_name','')} {addr.get('last_name','')}".strip()
        
        result.append({
            "id": o["id"],
            "name": o.get("name",""),
            "created_at": o.get("created_at",""),
            "customer": customer,
            "country_code": addr.get("country_code",""),
            "total": o.get("total_price","0"),
            "financial_status": o.get("financial_status",""),
            "fulfillment_status": o.get("fulfillment_status") or "unfulfilled",
            "items_count": sum(li.get("quantity",1) for li in o.get("line_items",[])),
            "source": o.get("source_name","online"),
            "tracking": tracking,
        })
    
    return result

@app.get("/config")
def get_config():
    cfg = load_config()
    return {"configured": bool(cfg.get("shop") and cfg.get("token")),
            "shop": cfg.get("shop",""), "shop_name": cfg.get("shop_name","")}

class ConfigIn(BaseModel):
    shop: str
    token: str

@app.post("/config")
async def set_config(body: ConfigIn):
    shop = body.shop.strip().rstrip("/")
    if not shop.endswith(".myshopify.com"):
        if "." not in shop:
            shop = f"{shop}.myshopify.com"
    # Test bağlantısı
    try:
        data, _ = await shopify_get(shop, body.token, "shop.json")
        shop_name = data.get("shop",{}).get("name", shop)
    except Exception as e:
        raise HTTPException(400, f"Bağlantı uğursuz: {e}")
    save_config({"shop": shop, "token": body.token, "shop_name": shop_name})
    return {"ok": True, "shop_name": shop_name}

@app.get("/orders")
async def get_orders():
    cfg = load_config()
    if not cfg.get("shop"):
        raise HTTPException(400, "Əvvəlcə Shopify konfiqurasiya edin")
    orders = await fetch_all_orders(cfg["shop"], cfg["token"])
    result = [transform_order(o) for o in orders]
    # Lokal şəkilləri yüklə və uyğunlaşdır
    images = load_images()
    for o in result:
        for i in o["items"]:
            i["image"] = match_image(i["name"], images)
            for c in i.get("box_contents", []):
                c["image"] = match_image(c["name"], images)
    return result

class FulfillBody(BaseModel):
    order_id: int
    order_name: str

@app.post("/fulfill")
async def fulfill_order(body: FulfillBody):
    save_archive_item(body.order_id, body.order_name)
    return {"ok": True}

@app.get("/archive")
def get_archive():
    return load_archive()

@app.delete("/archive/{order_id}")
def delete_from_archive(order_id: int):
    delete_archive_item(order_id)
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7000)
