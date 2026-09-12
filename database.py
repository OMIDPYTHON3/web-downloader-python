import aiosqlite

DB = "webdownloader.db"

async def init_db():
    async with aiosqlite.connect(DB) as d:
        await d.execute("""CREATE TABLE IF NOT EXISTS downloads(
        id INTEGER PRIMARY KEY,url TEXT NOT NULL,filename TEXT,filepath TEXT,total_size INTEGER DEFAULT 0,
        downloaded_size INTEGER DEFAULT 0,status TEXT DEFAULT 'Waiting',threads INTEGER DEFAULT 8,
        speed REAL DEFAULT 0,average_speed REAL DEFAULT 0,eta INTEGER DEFAULT 0,interface TEXT DEFAULT 'auto',
        error TEXT DEFAULT '',scheduled_at TEXT, schedule_end_at TEXT, queue_order INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,started_at TEXT,completed_at TEXT,category TEXT DEFAULT 'other')""")
        cols={r[1] for r in await (await d.execute("PRAGMA table_info(downloads)")).fetchall()}
        migrations={
            "interface":"ALTER TABLE downloads ADD COLUMN interface TEXT DEFAULT 'auto'",
            "category":"ALTER TABLE downloads ADD COLUMN category TEXT DEFAULT 'other'",
            "schedule_end_at":"ALTER TABLE downloads ADD COLUMN schedule_end_at TEXT",
            "queue_order":"ALTER TABLE downloads ADD COLUMN queue_order INTEGER DEFAULT 0",
        }
        for col,sql in migrations.items():
            if col not in cols:
                await d.execute(sql)
        await d.execute("UPDATE downloads SET queue_order=id WHERE queue_order IS NULL OR queue_order=0")
        await d.commit()

async def add(x):
    async with aiosqlite.connect(DB) as d:
        c=await d.execute("SELECT COALESCE(MAX(queue_order),0)+1 FROM downloads")
        order=(await c.fetchone())[0]
        c=await d.execute("""INSERT INTO downloads(url,filename,filepath,threads,interface,status,scheduled_at,schedule_end_at,queue_order,category)
        VALUES(?,?,?,?,?,?,?,?,?,?)""",(x["url"],x["filename"],x["filepath"],x["threads"],x["interface"],x["status"],x.get("scheduled_at"),x.get("schedule_end_at"),order,x.get("category","other")))
        await d.commit();return c.lastrowid

async def add_unique(x):
    """Atomically reject an existing URL and insert a new item otherwise."""
    async with aiosqlite.connect(DB) as d:
        await d.execute("BEGIN IMMEDIATE")
        c=await d.execute("SELECT id FROM downloads WHERE url=? LIMIT 1",(x["url"],))
        if await c.fetchone():
            await d.rollback(); return None
        c=await d.execute("SELECT COALESCE(MAX(queue_order),0)+1 FROM downloads")
        order=(await c.fetchone())[0]
        c=await d.execute("""INSERT INTO downloads(url,filename,filepath,threads,interface,status,scheduled_at,schedule_end_at,queue_order,category)
        VALUES(?,?,?,?,?,?,?,?,?,?)""",(x["url"],x["filename"],x["filepath"],x["threads"],x["interface"],x["status"],x.get("scheduled_at"),x.get("schedule_end_at"),order,x.get("category","other")))
        await d.commit();return c.lastrowid

async def add_bulk(items):
    """Insert in input order, atomically per item, returning added ids and duplicate URLs."""
    added=[];dups=[]
    async with aiosqlite.connect(DB) as d:
        await d.execute("BEGIN IMMEDIATE")
        c=await d.execute("SELECT COALESCE(MAX(queue_order),0) FROM downloads")
        order=(await c.fetchone())[0]
        for x in items:
            c=await d.execute("SELECT id FROM downloads WHERE url=? LIMIT 1",(x["url"],))
            if await c.fetchone():
                dups.append(x["url"]);continue
            order+=1
            c=await d.execute("""INSERT INTO downloads(url,filename,filepath,threads,interface,status,scheduled_at,schedule_end_at,queue_order,category)
            VALUES(?,?,?,?,?,?,?,?,?,?)""",(x["url"],x["filename"],x["filepath"],x["threads"],x["interface"],x["status"],x.get("scheduled_at"),x.get("schedule_end_at"),order,x.get("category","other")))
            added.append(c.lastrowid)
        await d.commit()
    return added,dups

async def all():
    async with aiosqlite.connect(DB) as d:
        d.row_factory=aiosqlite.Row
        return [dict(x) for x in await (await d.execute("SELECT * FROM downloads ORDER BY queue_order ASC,id ASC")).fetchall()]

async def get(i):
    async with aiosqlite.connect(DB) as d:
        d.row_factory=aiosqlite.Row;r=await (await d.execute("SELECT * FROM downloads WHERE id=?",(i,))).fetchone()
        return dict(r) if r else None

async def update(i,**kw):
    if not kw:return
    allowed={"url","filename","filepath","total_size","downloaded_size","status","threads","speed","average_speed","eta","interface","error","scheduled_at","schedule_end_at","queue_order","started_at","completed_at","category"}
    kw={k:v for k,v in kw.items() if k in allowed}
    if not kw:return
    async with aiosqlite.connect(DB) as d:
        await d.execute("UPDATE downloads SET "+",".join(f"{k}=?" for k in kw)+" WHERE id=?",(*kw.values(),i));await d.commit()

async def remove(i):
    async with aiosqlite.connect(DB) as d:await d.execute("DELETE FROM downloads WHERE id=?",(i,));await d.commit()
