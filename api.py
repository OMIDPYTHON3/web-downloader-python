import asyncio,json,os,re,subprocess
from urllib.parse import urlparse
from fastapi import FastAPI,Request
from fastapi.responses import HTMLResponse,StreamingResponse
from fastapi.staticfiles import StaticFiles
from database import *
from downloader import Engine,safe_name
from config import load,save

cfg=load();app=FastAPI();app.mount('/static',StaticFiles(directory='web'),name='static');waiters=[]

def valid(u):
    try:
        p=urlparse(u); return p.scheme in ('http','https') and bool(p.netloc)
    except: return False

def ifaces():
    a=[{'name':'auto','ip':'Automatic'}]
    try:
        for l in subprocess.check_output(['ip','-o','-4','addr','show'],text=True).splitlines():
            p=l.split(); m=re.search(r'(\d+\.\d+\.\d+\.\d+)/',p[3]) if len(p)>3 else None
            if m and p[1] != 'lo': a.append({'name':p[1],'ip':m.group(1)})
    except: pass
    return a

async def emit():
    x=json.dumps(await all(),separators=(',',':'))
    for q in list(waiters):
        try:q.put_nowait(x)
        except:pass

def roots():
    vals=cfg.get('download_roots') or [cfg['download_dir']]
    vals=[os.path.abspath(str(x)) for x in vals]
    base=os.path.abspath(cfg['download_dir'])
    if base not in vals: vals.insert(0,base)
    return list(dict.fromkeys(vals))

def allowed(path):
    p=os.path.abspath(path)
    return any(p==r or p.startswith(r+os.sep) for r in roots())

def choose_folder(x):
    folder=os.path.abspath(os.path.expanduser(x.get('folder') or cfg['download_dir']))
    if not allowed(folder): folder=os.path.abspath(cfg['download_dir'])
    os.makedirs(folder,exist_ok=True)
    return folder

def detect_category(filename, url=''):
    ext=os.path.splitext(safe_name(filename or urlparse(url).path.split('/')[-1]))[1].lower()
    groups={'video':{'.mp4','.mkv','.webm','.avi','.mov','.m4v','.ts','.flv','.wmv','.3gp'},'audio':{'.mp3','.flac','.aac','.m4a','.wav','.ogg','.opus','.alac'},'document':{'.pdf','.doc','.docx','.xls','.xlsx','.ppt','.pptx','.txt','.csv','.epub','.srt'},'archive':{'.zip','.rar','.7z','.tar','.gz','.bz2','.xz','.iso'},'image':{'.jpg','.jpeg','.png','.gif','.webp','.bmp','.svg','.heic'},'app':{'.apk','.exe','.deb','.rpm','.msi','.dmg'}}
    for cat,exts in groups.items():
        if ext in exts:return cat
    return 'other'

def normalize_schedule(v):
    if not v:return None
    v=str(v).strip()
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        dt=datetime.fromisoformat(v.replace('Z','+00:00'))
        if dt.tzinfo is None: dt=dt.replace(tzinfo=ZoneInfo('Asia/Tehran'))
        else: dt=dt.astimezone(ZoneInfo('Asia/Tehran'))
        return dt.strftime('%Y-%m-%dT%H:%M:%S')
    except: return None

def build_item(x):
    folder=choose_folder(x);u=x['url'];name=safe_name(x.get('filename') or urlparse(u).path.split('/')[-1])
    start=normalize_schedule(x.get('scheduled_at')); end=normalize_schedule(x.get('schedule_end_at'))
    if start and not end: raise ValueError('Schedule end time is required when scheduling is enabled')
    if end and not start: raise ValueError('Schedule start time is required when scheduling is enabled')
    if start and end:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        try:
            if datetime.fromisoformat(end).replace(tzinfo=ZoneInfo('Asia/Tehran')) <= datetime.fromisoformat(start).replace(tzinfo=ZoneInfo('Asia/Tehran')):
                raise ValueError('Schedule end must be after start')
        except ValueError: raise
        except Exception: end=None
    return {'url':u,'filename':name,'filepath':os.path.join(folder,name),'threads':max(1,min(32,int(x.get('threads',cfg['default_threads'])))),'interface':x.get('interface','auto'),'status':'Scheduled' if start else 'Waiting','scheduled_at':start,'schedule_end_at':end,'category':(x.get('category') if x.get('category') not in (None,'','auto') else detect_category(name,u))}

@app.on_event('startup')
async def startup():
    await init_db(); global engine; engine=Engine(cfg,emit); await engine.restore()

@app.get('/',response_class=HTMLResponse)
async def home(): return open('web/index.html',encoding='utf8').read()
@app.get('/api/downloads')
async def dl(): return await all()
@app.get('/api/interfaces')
async def interfaces(): return ifaces()

@app.get('/api/browse')
async def browse(path:str=''):
    requested=os.path.abspath(os.path.expanduser(path or cfg['download_dir']))
    if not allowed(requested): requested=os.path.abspath(cfg['download_dir'])
    try:
        os.makedirs(requested,exist_ok=True);dirs=[]
        for e in sorted(os.scandir(requested),key=lambda x:x.name.lower()):
            if e.is_dir() and not e.is_symlink(): dirs.append({'name':e.name,'path':e.path})
        parent=os.path.dirname(requested);parent=parent if allowed(parent) else None
        return {'path':requested,'parent':parent,'dirs':dirs,'roots':roots()}
    except Exception as e:return {'path':requested,'parent':None,'dirs':[],'roots':roots(),'error':str(e)}

@app.post('/api/downloads')
async def addone(r:Request):
    x=await r.json()
    if not valid(x.get('url','')): return {'error':'Only HTTP/HTTPS URLs are allowed'}
    try:item=build_item(x)
    except Exception as e:return {'error':str(e)}
    if os.path.exists(item['filepath']) and not os.path.exists(item['filepath']+'.part'):
        return {'error':'Duplicate file already exists at destination'}
    i=await add_unique(item)
    if i is None:return {'error':'Duplicate download: this URL is already in the list'}
    await emit();return await get(i)

@app.post('/api/downloads/bulk')
async def bulk(r:Request):
    x=await r.json();raw=x.get('urls',[])
    cleaned=[u.strip() for u in raw if isinstance(u,str) and u.strip()]
    good=[];invalid=0;seen=set()
    for u in cleaned:
        if not valid(u): invalid+=1;continue
        if u in seen: continue
        seen.add(u);good.append(u)
    items=[];file_dups=0;batch_paths=set()
    for u in good:
        try:
            it=build_item({**x,'url':u})
            fp=os.path.abspath(it['filepath'])
            if fp in batch_paths or (os.path.exists(fp) and not os.path.exists(fp+'.part')):
                file_dups+=1;continue
            batch_paths.add(fp);items.append(it)
        except Exception:
            invalid+=1
    ids,dups=await add_bulk(items)
    await emit();return {'added':len(ids),'duplicates':len(dups)+file_dups,'invalid':invalid,'ids':ids,'duplicate_urls':dups}

@app.post('/api/control/{act}')
async def control_all(act:str):
    rows=await all()
    if act=='pause-all':
        for d in rows:
            if d['status'] in ('Downloading','Waiting','Retrying'):
                await engine.pause(d['id'])
    elif act=='start-all':
        for d in rows:
            if d['status'] in ('Paused','Error'):
                await engine.resume(d['id'])
            elif d['status']=='Waiting':
                await engine.resume(d['id'])
    else:return {'error':'Unknown control'}
    await emit();return {'ok':True}

@app.post('/api/downloads/{i}/redownload')
async def redownload(i:int):
    d=await get(i)
    if not d: return {'error':'Download not found'}

    # Stop an existing worker and wait for it to finish before touching files.
    t=engine.running.get(i)
    if t:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    def clean():
        for path in [d['filepath'], d['filepath']+'.part']:
            try: os.remove(path)
            except FileNotFoundError: pass
        for n in range(32):
            try: os.remove(d['filepath']+f'.seg{n}')
            except FileNotFoundError: pass

    await asyncio.to_thread(clean)
    await update(i,status='Waiting',downloaded_size=0,total_size=0,speed=0,average_speed=0,eta=0,error='',started_at=None,completed_at=None)
    await emit()

    # Re-enter the normal scheduler so max_concurrent/FIFO rules are preserved.
    await engine.resume(i)
    return await get(i)

@app.post('/api/downloads/{i}/{act}')
async def action(i:int,act:str):
    if act=='pause':await engine.pause(i)
    elif act in ('resume','retry'):await engine.resume(i)
    elif act=='cancel':await engine.cancel(i)
    else:return {'error':'Unknown action'}
    return await get(i)

@app.put('/api/downloads/{i}')
async def edit_download(i:int,r:Request):
    d=await get(i)
    if not d: return {'error':'Download not found'}
    x=await r.json()
    allowed={'filename','threads','interface','category','scheduled_at','schedule_end_at'}
    changes={k:x[k] for k in allowed if k in x}
    if 'threads' in changes:
        changes['threads']=max(1,min(32,int(changes['threads'])))
    if 'filename' in changes:
        changes['filename']=safe_name(changes['filename'])
        newpath=os.path.join(os.path.dirname(d['filepath']),changes['filename'])
        if newpath!=d['filepath'] and os.path.exists(newpath): return {'error':'A file with this name already exists'}
        if newpath!=d['filepath'] and os.path.exists(d['filepath']):
            await asyncio.to_thread(os.rename,d['filepath'],newpath)
        changes['filepath']=newpath
    was_running=i in engine.running
    if was_running:
        await engine.pause(i)
    if changes: await update(i,**changes)
    if was_running:
        await update(i,status='Waiting',error='')
    await emit(); return await get(i)

@app.delete('/api/downloads/{i}')
async def rem(i:int):
    d=await get(i);await engine.cancel(i);await remove(i)
    if d:
        for p in (d['filepath'],d['filepath']+'.part'):
            try:os.remove(p)
            except:pass
        for n in range(32):
            try:os.remove(d['filepath']+f'.seg{n}')
            except:pass
    await emit();return {'ok':True}

@app.get('/api/settings')
async def getset():return cfg
@app.put('/api/settings')
async def setset(r:Request):
    x=await r.json()
    for k in ('download_dir','max_concurrent','default_threads','retry_count','retry_delay','auto_resume','smart_download','download_roots'):
        if k in x:cfg[k]=x[k]
    cfg['max_concurrent']=max(1,min(8,int(cfg.get('max_concurrent',4))))
    cfg['default_threads']=max(1,min(32,int(cfg.get('default_threads',8))))
    cfg['download_dir']=os.path.abspath(cfg['download_dir']);os.makedirs(cfg['download_dir'],exist_ok=True)
    cfg['download_roots']=list(dict.fromkeys([os.path.abspath(os.path.expanduser(str(v))) for v in cfg.get('download_roots',[])]+[cfg['download_dir']]))
    for p in cfg['download_roots']:
        try:os.makedirs(p,exist_ok=True)
        except:pass
    save(cfg);engine.cfg=cfg;engine.set_concurrency(int(cfg['max_concurrent']));return cfg

@app.get('/api/events')
async def events():
    q=asyncio.Queue();waiters.append(q)
    async def gen():
        try:
            yield 'data: '+json.dumps(await all())+'\n\n'
            while True:yield 'data: '+await q.get()+'\n\n'
        finally:
            if q in waiters:waiters.remove(q)
    return StreamingResponse(gen(),media_type='text/event-stream',headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no'})
