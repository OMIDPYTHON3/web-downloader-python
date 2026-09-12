import asyncio,aiohttp,os,re,time,shutil,subprocess,socket
from urllib.parse import urlparse,unquote
from datetime import datetime
from zoneinfo import ZoneInfo
from database import all,get,update

def safe_name(s):
    s=os.path.basename(unquote(s or 'download'));s=re.sub(r'[\x00-\x1f<>:"/\\|?*]+','_',s).strip(' .');return (s or 'download')[:240]
def interface_ip(name):
    if not name or name=='auto':return None
    try:
        out=subprocess.check_output(['ip','-4','-o','addr','show','dev',name],text=True,stderr=subprocess.DEVNULL)
        m=re.search(r'inet\s+(\d+\.\d+\.\d+\.\d+)',out);return m.group(1) if m else None
    except:return None
def free(path):return shutil.disk_usage(os.path.dirname(os.path.abspath(path))).free

class Engine:
    def __init__(self,cfg,emit):
        self.cfg=cfg;self.emit=emit;self.running={};self.sem=asyncio.Semaphore(int(cfg['max_concurrent']))
    def set_concurrency(self,n): self.sem=asyncio.Semaphore(max(1,n))
    async def restore(self):
        for d in await all():
            if d['status'] in ('Downloading','Retrying') and self.cfg.get('auto_resume',True): await update(d['id'],status='Waiting',speed=0,eta=0)
        asyncio.create_task(self.loop())
    async def loop(self):
        while True:
            try:
                now=time.time()
                for d in await all():
                    # A scheduled download has a hard end boundary. Once the
                    # end time is reached, stop the active task or prevent a
                    # waiting task from starting. Partial data is preserved.
                    if d.get('schedule_end_at') and d['status'] in ('Downloading','Waiting','Retrying'):
                        try:
                            end_dt=datetime.fromisoformat(d['schedule_end_at']).replace(tzinfo=ZoneInfo('Asia/Tehran'))
                            if end_dt.timestamp() <= now:
                                if d['id'] in self.running:
                                    await self.pause(d['id'])
                                else:
                                    await update(d['id'],status='Paused',speed=0,eta=0,error='Schedule end time reached; download paused')
                                continue
                        except Exception:
                            pass
                    if d['status']=='Scheduled' and d['scheduled_at']:
                        try:
                            dt=datetime.fromisoformat(d['scheduled_at']).replace(tzinfo=ZoneInfo('Asia/Tehran'))
                            if dt.timestamp()<=now: await update(d['id'],status='Waiting')
                        except: pass
                rows=await all()
                for d in rows:
                    if d['status']=='Waiting' and d['id'] not in self.running: await self.start(d['id'])
            except Exception: pass
            await asyncio.sleep(.5)
    async def start(self,i):
        if i in self.running:return
        async def run():
            async with self.sem:
                try: await self.download(i)
                except asyncio.CancelledError: raise
                except Exception as e:
                    await update(i,status='Error',error=str(e),speed=0,eta=0); await self.emit()
        t=asyncio.create_task(run());self.running[i]=t
        def done(_): self.running.pop(i,None)
        t.add_done_callback(done)
    async def pause(self,i):
        t=self.running.get(i)
        if t:t.cancel()
        await update(i,status='Paused',speed=0,eta=0);await self.emit()
    async def cancel(self,i):
        t=self.running.get(i)
        if t:t.cancel()
        await update(i,status='Cancelled',speed=0,eta=0);await self.emit()
    async def resume(self,i):
        t=self.running.get(i)
        if t:t.cancel()
        await update(i,status='Waiting',error='',speed=0,eta=0);await self.emit()
    async def download(self,i):
        d=await get(i);path=d['filepath'];os.makedirs(os.path.dirname(path),exist_ok=True)
        # Binding only the source IP is not enough when eth0 and wlan0 share
        # the same subnet: Linux may still choose the main-table default route.
        # Bind the actual socket to the selected interface with SO_BINDTODEVICE.
        iface=(d.get('interface') or 'auto').strip()
        bind=interface_ip(iface)
        socket_factory=None
        if iface != 'auto':
            def make_socket(addr_info, _iface=iface):
                family, socktype, proto = addr_info[0], addr_info[1], addr_info[2]
                sock=socket.socket(family=family, type=socktype, proto=proto)
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE, _iface.encode()+b'\0')
                except OSError as e:
                    sock.close()
                    raise RuntimeError(f'Cannot bind download to interface {_iface}: {e}')
                return sock
            socket_factory=make_socket
        conn=aiohttp.TCPConnector(
            local_addr=(bind,0) if bind else None,
            socket_factory=socket_factory,
            limit=0,
            force_close=False
        )
        timeout=aiohttp.ClientTimeout(total=None,connect=30,sock_read=60)
        try:
            async with aiohttp.ClientSession(connector=conn,timeout=timeout) as s:
                async with s.head(d['url'],allow_redirects=True) as r:
                    total=int(r.headers.get('Content-Length') or 0);accept=r.headers.get('Accept-Ranges','').lower()=='bytes'
                    if r.status>=400: raise RuntimeError(f'HTTP {r.status}')
                if not total:
                    async with s.get(d['url'],allow_redirects=True,headers={'Range':'bytes=0-0','Accept-Encoding':'identity'}) as r:
                        if r.status>=400: raise RuntimeError(f'HTTP {r.status}')
                        if r.status==206 and r.headers.get('Content-Range'):
                            m=re.search(r'/([0-9]+)$',r.headers['Content-Range']);total=int(m.group(1)) if m else total;accept=True
                if total and free(path)<max(0,total-(os.path.getsize(path) if os.path.exists(path) else 0)): raise RuntimeError('Insufficient disk space')
                await update(i,total_size=total,status='Downloading',started_at=d['started_at'] or time.strftime('%Y-%m-%dT%H:%M:%S'),error='');await self.emit()
                if accept and total and d['threads']>1: await self.ranged(s,d['url'],path,total,d['threads'],i)
                else: await self.single(s,d['url'],path,total,i)
        except asyncio.CancelledError: raise
        except Exception as e: await update(i,status='Error',error=str(e),speed=0,eta=0);await self.emit();return
        q=await get(i)
        await update(i,status='Completed',downloaded_size=q['total_size'] or q['downloaded_size'],speed=0,eta=0,completed_at=time.strftime('%Y-%m-%dT%H:%M:%S'),error='');await self.emit()
    async def single(self,s,url,path,total,i):
        # A connection can legitimately die before the advertised Content-Length.
        # Never turn that into a fatal download error: keep the partial file and
        # resume it with HTTP Range until retry_count is exhausted.
        done=os.path.getsize(path) if os.path.exists(path) else 0
        speed=0; last_done=done; last=time.monotonic()
        retries=int(self.cfg.get('retry_count',5))
        delay=float(self.cfg.get('retry_delay',3))
        for attempt in range(retries+1):
            try:
                if total and done>=total:
                    break
                h={'Accept-Encoding':'identity'}
                if done>0:
                    h['Range']=f'bytes={done}-'
                async with s.get(url,headers=h,allow_redirects=True) as r:
                    if r.status not in (200,206):
                        raise RuntimeError(f'HTTP {r.status}')
                    if done>0 and r.status==200:
                        # Server ignored Range. Restart only when we know this is
                        # the full response; otherwise retry rather than corrupting.
                        done=0
                        mode='wb'
                    else:
                        mode='ab' if done else 'wb'
                    expected=None
                    if r.status==206:
                        cr=r.headers.get('Content-Range','')
                        m=re.match(r'bytes\s+(\d+)-(\d+)/(\d+|\*)',cr)
                        if m:
                            expected=int(m.group(2))-int(m.group(1))+1
                            if int(m.group(1))!=done:
                                raise RuntimeError('Server returned an unexpected Range offset')
                    else:
                        expected=int(r.headers.get('Content-Length') or 0) or None
                    received=0
                    with open(path,mode) as f:
                        async for b in r.content.iter_chunked(256*1024):
                            f.write(b);received+=len(b);done+=len(b);now=time.monotonic()
                            if now-last>=1.0:
                                speed=(done-last_done)/(now-last);last_done=done;last=now
                                eta=int((total-done)/speed) if speed>0 and total else 0
                                await update(i,downloaded_size=done,speed=max(0,speed),eta=eta);await self.emit()
                    if expected is not None and received!=expected:
                        raise aiohttp.ClientPayloadError(
                            f'Incomplete response body: received {received} of {expected} bytes')
                if not total or done>=total:
                    break
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError, asyncio.TimeoutError, OSError) as e:
                if attempt>=retries:
                    raise RuntimeError(f'Download interrupted after {retries+1} attempts: {e}')
                await update(i,downloaded_size=done,speed=0,eta=0,error=f'Connection interrupted; retrying ({attempt+1}/{retries})')
                await self.emit()
                await asyncio.sleep(delay)
            except Exception:
                raise
        await update(i,downloaded_size=done,speed=0,eta=0)
    async def ranged(self,s,url,path,total,n,i):
        with open(path,'ab') as f:f.truncate(total)
        step=(total+n-1)//n;progress=[0]*n;last_total=0;last=time.monotonic()
        for k in range(n):
            side=path+f'.seg{k}'
            try: progress[k]=int(open(side).read())
            except: progress[k]=0
        async def publish(force=False):
            nonlocal last_total,last
            now=time.monotonic();cur=sum(progress)
            if force or now-last>=1.0:
                sp=(cur-last_total)/(now-last) if now>last else 0;eta=int((total-cur)/sp) if sp>0 else 0
                await update(i,downloaded_size=cur,speed=max(0,sp),eta=eta);await self.emit();last_total=cur;last=now
        async def worker(k):
            lo=k*step;hi=min(total-1,(k+1)*step-1);done=max(0,progress[k]);
            if lo+done>hi:return
            for attempt in range(int(self.cfg['retry_count'])+1):
                try:
                    async with s.get(url,headers={'Range':f'bytes={lo+done}-{hi}','Accept-Encoding':'identity'},allow_redirects=True) as r:
                        if r.status!=206: raise RuntimeError('Server does not support HTTP Range')
                        pos=lo+done
                        with open(path,'r+b',buffering=0) as f:
                            while True:
                                b=await r.content.read(256*1024)
                                if not b:break
                                f.seek(pos);f.write(b);pos+=len(b);done=pos-lo;progress[k]=done
                                if done%(2*1024*1024)<len(b):
                                    with open(path+f'.seg{k}','w') as z:z.write(str(done))
                                await publish()
                    try:os.remove(path+f'.seg{k}')
                    except:pass
                    return
                except asyncio.CancelledError: raise
                except Exception as e:
                    if attempt>=int(self.cfg.get('retry_count',5)):
                        raise RuntimeError(f'Segment {k} interrupted after {attempt+1} attempts: {e}')
                    await asyncio.sleep(float(self.cfg.get('retry_delay',3)))
        await asyncio.gather(*(worker(k) for k in range(n)));await publish(True);await update(i,downloaded_size=total,speed=0,eta=0)
