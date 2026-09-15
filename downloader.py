import asyncio
import aiohttp
import os
import re
import time
import shutil
import subprocess
import socket
from urllib.parse import unquote
from datetime import datetime
from zoneinfo import ZoneInfo
from database import all, get, update


CHUNK = 256 * 1024
SMART_MIN = 2
SMART_STEP_INTERVAL = 2.0


def safe_name(s):
    s = os.path.basename(unquote(s or 'download'))
    s = re.sub(r'[\x00-\x1f<>:"/\\|?*]+', '_', s).strip(' .')
    return (s or 'download')[:240]


def interface_ip(name):
    if not name or name == 'auto':
        return None
    try:
        out = subprocess.check_output(
            ['ip', '-4', '-o', 'addr', 'show', 'dev', name],
            text=True, stderr=subprocess.DEVNULL
        )
        m = re.search(r'inet\s+(\d+\.\d+\.\d+\.\d+)', out)
        return m.group(1) if m else None
    except Exception:
        return None


def free(path):
    return shutil.disk_usage(os.path.dirname(os.path.abspath(path))).free



class SmartController:
    """Adaptive connection gate. It changes concurrency from real throughput/errors."""
    def __init__(self, maximum, enabled=True):
        self.maximum = max(1, int(maximum))
        self.enabled = bool(enabled)
        self.target = min(self.maximum, 4) if self.enabled else self.maximum
        self.active = 0
        self.condition = asyncio.Condition()
        self.last_sample = time.monotonic()
        self.last_speed = 0.0
        self.sample_bytes = 0
        self.errors = 0
        self.reason = 'initial adaptive target'

    async def enter(self):
        async with self.condition:
            while self.active >= self.target:
                await self.condition.wait()
            self.active += 1

    async def leave(self):
        async with self.condition:
            self.active = max(0, self.active - 1)
            self.condition.notify_all()

    async def add_bytes(self, n):
        self.sample_bytes += n

    async def error(self):
        self.errors += 1

    async def tune(self, force=False):
        if not self.enabled or self.maximum <= 1:
            return
        now = time.monotonic()
        elapsed = now - self.last_sample
        if not force and elapsed < SMART_STEP_INTERVAL:
            return
        speed = self.sample_bytes / elapsed if elapsed > 0 else 0.0
        old = self.target
        if self.errors >= 2 and self.target > SMART_MIN:
            self.target -= 1
            self.reason = 'errors/timeouts increased'
        elif self.errors == 0 and speed > 0:
            if self.last_speed <= 0 or speed > self.last_speed * 1.10:
                if self.target < self.maximum:
                    self.target += 1
                    self.reason = 'throughput improved'
            elif self.last_speed > 0 and speed < self.last_speed * 0.80 and self.target > SMART_MIN:
                self.target -= 1
                self.reason = 'throughput dropped'
        if self.target != old:
            async with self.condition:
                self.condition.notify_all()
        self.last_speed = speed
        self.sample_bytes = 0
        self.errors = 0
        self.last_sample = now


class Engine:
    def __init__(self, cfg, emit):
        self.cfg = cfg
        self.emit = emit
        self.running = {}
        self.max_concurrent = max(1, int(cfg.get('max_concurrent', 4)))
        self.slot_condition = asyncio.Condition()
        self.active_slots = 0
        self.smart_status = {}

    async def _acquire_slot(self):
        async with self.slot_condition:
            while self.active_slots >= self.max_concurrent:
                await self.slot_condition.wait()
            self.active_slots += 1

    async def _release_slot(self):
        async with self.slot_condition:
            self.active_slots = max(0, self.active_slots - 1)
            self.slot_condition.notify_all()

    def set_concurrency(self, n):
        self.max_concurrent = max(1, int(n))
        async def wake():
            async with self.slot_condition:
                self.slot_condition.notify_all()
        try: asyncio.create_task(wake())
        except RuntimeError: pass

    async def restore(self):
        for d in await all():
            if d['status'] in ('Downloading', 'Retrying') and self.cfg.get('auto_resume', True):
                await update(d['id'], status='Waiting', speed=0, eta=0)
        asyncio.create_task(self.loop())

    async def loop(self):
        while True:
            try:
                now = time.time()
                for d in await all():
                    if d.get('schedule_end_at') and d['status'] in ('Downloading', 'Waiting', 'Retrying'):
                        try:
                            end_dt = datetime.fromisoformat(d['schedule_end_at']).replace(tzinfo=ZoneInfo('Asia/Tehran'))
                            if end_dt.timestamp() <= now:
                                if d['id'] in self.running:
                                    await self.pause(d['id'])
                                else:
                                    await update(d['id'], status='Paused', speed=0, eta=0,
                                                 error='Schedule end time reached; download paused')
                                continue
                        except Exception:
                            pass
                    if d['status'] == 'Scheduled' and d['scheduled_at']:
                        try:
                            dt = datetime.fromisoformat(d['scheduled_at']).replace(tzinfo=ZoneInfo('Asia/Tehran'))
                            if dt.timestamp() <= now:
                                await update(d['id'], status='Waiting')
                        except Exception:
                            pass
                rows = await all()
                # Strict FIFO admission: only launch as many queue entries as
                # there are available download slots. New entries can never
                # bypass older Waiting entries, and max_concurrent=1 really means 1.
                async with self.slot_condition:
                    available = max(0, self.max_concurrent - self.active_slots)
                for d in rows:
                    if available <= 0:
                        break
                    if d['status'] == 'Waiting' and d['id'] not in self.running:
                        await self.start(d['id'])
                        available -= 1
            except Exception:
                pass
            await asyncio.sleep(0.5)

    async def start(self, i):
        if i in self.running:
            return False
        await self._acquire_slot()

        started = [False]
        async def run():
            started[0] = True
            try:
                await self.download(i)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await update(i, status='Error', error=str(e), speed=0, eta=0)
                await self.emit()
            finally:
                await self._release_slot()
                self.running.pop(i, None)
                await self.emit()

        t = asyncio.create_task(run())
        self.running[i] = t
        def done_callback(_):
            # If cancellation happened before the coroutine got CPU time,
            # its finally block never runs; release the reserved slot here.
            if not started[0]:
                try: asyncio.create_task(self._release_slot())
                except RuntimeError: pass
            self.running.pop(i, None)
        t.add_done_callback(done_callback)
        return True

    async def pause(self, i):
        t = self.running.get(i)
        if t:
            t.cancel()
        await update(i, status='Paused', speed=0, eta=0)
        await self.emit()

    async def cancel(self, i):
        t = self.running.get(i)
        if t:
            t.cancel()
        await update(i, status='Cancelled', speed=0, eta=0)
        await self.emit()

    async def resume(self, i):
        t = self.running.get(i)
        if t:
            t.cancel()
        await update(i, status='Waiting', error='', speed=0, eta=0)
        await self.emit()

    def _connector(self, iface, bind=None):
        socket_factory = None
        if iface != 'auto':
            def make_socket(addr_info, _iface=iface):
                family, socktype, proto = addr_info[0], addr_info[1], addr_info[2]
                sock = socket.socket(family=family, type=socktype, proto=proto)
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE, _iface.encode() + b'\0')
                except OSError as e:
                    sock.close()
                    raise RuntimeError(f'Cannot bind download to interface {_iface}: {e}')
                return sock
            socket_factory = make_socket
        return aiohttp.TCPConnector(
            local_addr=(bind, 0) if bind else None,
            socket_factory=socket_factory,
            limit=0,
            force_close=False,
            ttl_dns_cache=300,
        )

    async def download(self, i):
        d = await get(i)
        path = d['filepath']
        await asyncio.to_thread(os.makedirs, os.path.dirname(path), exist_ok=True)
        iface = (d.get('interface') or 'auto').strip()
        bind = await asyncio.to_thread(interface_ip, iface) if iface != 'auto' else None
        conn = self._connector(iface, bind)
        timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=60)
        try:
            async with aiohttp.ClientSession(connector=conn, timeout=timeout) as s:
                async with s.head(d['url'], allow_redirects=True) as r:
                    total = int(r.headers.get('Content-Length') or 0)
                    accept = r.headers.get('Accept-Ranges', '').lower() == 'bytes'
                    if r.status >= 400:
                        raise RuntimeError(f'HTTP {r.status}')
                if not total:
                    async with s.get(d['url'], allow_redirects=True,
                                     headers={'Range': 'bytes=0-0', 'Accept-Encoding': 'identity'}) as r:
                        if r.status >= 400:
                            raise RuntimeError(f'HTTP {r.status}')
                        if r.status == 206 and r.headers.get('Content-Range'):
                            m = re.search(r'/([0-9]+)$', r.headers['Content-Range'])
                            total = int(m.group(1)) if m else total
                            accept = True
                existing = await asyncio.to_thread(os.path.getsize, path) if os.path.exists(path) else 0
                if total and await asyncio.to_thread(free, path) < max(0, total - existing):
                    raise RuntimeError('Insufficient disk space')
                await update(i, total_size=total, status='Downloading',
                             started_at=d['started_at'] or time.strftime('%Y-%m-%dT%H:%M:%S'), error='')
                await self.emit()
                smart = bool(self.cfg.get('smart_download', True))
                if accept and total and d['threads'] > 1:
                    await self.ranged(s, d['url'], path, total, d['threads'], i, smart)
                else:
                    await self.single(s, d['url'], path, total, i)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            await update(i, status='Error', error=str(e), speed=0, eta=0)
            await self.emit()
            return
        q = await get(i)
        await update(i, status='Completed', downloaded_size=q['total_size'] or q['downloaded_size'],
                     speed=0, eta=0, completed_at=time.strftime('%Y-%m-%dT%H:%M:%S'), error='')
        self.smart_status.pop(i, None)
        await self.emit()

    async def _write(self, f, data, offset=None):
        """Move blocking file writes off the asyncio event loop."""
        if offset is None:
            await asyncio.to_thread(f.write, data)
        else:
            await asyncio.to_thread(os.pwrite, f.fileno(), data, offset)

    async def single(self, s, url, path, total, i):
        done = os.path.getsize(path) if os.path.exists(path) else 0
        speed = 0.0
        last_done = done
        last = time.monotonic()
        retries = int(self.cfg.get('retry_count', 5))
        delay = float(self.cfg.get('retry_delay', 3))
        for attempt in range(retries + 1):
            try:
                if total and done >= total:
                    break
                h = {'Accept-Encoding': 'identity'}
                if done > 0:
                    h['Range'] = f'bytes={done}-'
                async with s.get(url, headers=h, allow_redirects=True) as r:
                    if r.status not in (200, 206):
                        raise RuntimeError(f'HTTP {r.status}')
                    if done > 0 and r.status == 200:
                        done = 0
                        mode = 'wb'
                    else:
                        mode = 'ab' if done else 'wb'
                    expected = None
                    if r.status == 206:
                        cr = r.headers.get('Content-Range', '')
                        m = re.match(r'bytes\s+(\d+)-(\d+)/(\d+|\*)', cr)
                        if m:
                            expected = int(m.group(2)) - int(m.group(1)) + 1
                            if int(m.group(1)) != done:
                                raise RuntimeError('Server returned an unexpected Range offset')
                    else:
                        expected = int(r.headers.get('Content-Length') or 0) or None
                    received = 0
                    with open(path, mode, buffering=1024 * 1024) as f:
                        async for b in r.content.iter_chunked(CHUNK):
                            await self._write(f, b)
                            received += len(b)
                            done += len(b)
                            now = time.monotonic()
                            if now - last >= 1.0:
                                speed = (done - last_done) / (now - last)
                                last_done, last = done, now
                                eta_s = int((total - done) / speed) if speed > 0 and total else 0
                                await update(i, downloaded_size=done, speed=max(0, speed), eta=eta_s)
                                await self.emit()
                    if expected is not None and received != expected:
                        raise aiohttp.ClientPayloadError(
                            f'Incomplete response body: received {received} of {expected} bytes')
                if not total or done >= total:
                    break
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError,
                    asyncio.TimeoutError, OSError) as e:
                if attempt >= retries:
                    raise RuntimeError(f'Download interrupted after {retries + 1} attempts: {e}')
                await update(i, downloaded_size=done, speed=0, eta=0,
                             error=f'Connection interrupted; retrying ({attempt + 1}/{retries})')
                await self.emit()
                await asyncio.sleep(delay)
        await update(i, downloaded_size=done, speed=0, eta=0)

    async def ranged(self, s, url, path, total, n, i, smart):
        # Sparse preallocation is near-instant on normal filesystems. The
        # potentially blocking syscall is explicitly moved off the event loop.
        if not os.path.exists(path) or os.path.getsize(path) != total:
            await asyncio.to_thread(self._ensure_sparse_file, path, total)

        max_conn = max(1, min(32, int(n)))
        # Smart mode gets twice as many smaller work units, so it can adapt
        # connection count while a large file is still downloading.
        parts = min(32, max_conn * 2) if smart and max_conn > 1 else max_conn
        step = (total + parts - 1) // parts
        progress = [0] * parts
        for k in range(parts):
            side = path + f'.seg{k}'
            try:
                with open(side, encoding='ascii') as z:
                    progress[k] = min(max(0, int(z.read().strip())), max(0, min(step, total - k * step)))
            except Exception:
                progress[k] = 0

        controller = SmartController(max_conn, smart)
        self.smart_status[i] = {
            'enabled': smart,
            'connections': controller.target,
            'reason': controller.reason,
        }
        lock = asyncio.Lock()
        last_total = sum(progress)
        last = time.monotonic()
        retries = int(self.cfg.get('retry_count', 5))
        delay = float(self.cfg.get('retry_delay', 3))

        async def publish(force=False):
            nonlocal last_total, last
            now = time.monotonic()
            cur = sum(progress)
            if force or now - last >= 1.0:
                sp = (cur - last_total) / (now - last) if now > last else 0
                eta_s = int((total - cur) / sp) if sp > 0 else 0
                await update(i, downloaded_size=cur, speed=max(0, sp), eta=eta_s)
                await self.emit()
                last_total, last = cur, now
            await controller.tune()
            self.smart_status[i] = {
                'enabled': smart,
                'connections': controller.target,
                'reason': controller.reason,
            }

        async def worker(k):
            lo = k * step
            hi = min(total - 1, (k + 1) * step - 1)
            done = max(0, progress[k])
            if lo + done > hi:
                return
            for attempt in range(retries + 1):
                await controller.enter()
                try:
                    async with s.get(
                        url,
                        headers={'Range': f'bytes={lo + done}-{hi}', 'Accept-Encoding': 'identity'},
                        allow_redirects=True,
                    ) as r:
                        if r.status != 206:
                            raise RuntimeError('Server does not support HTTP Range')
                        request_start = lo + done
                        pos = request_start
                        with open(path, 'r+b', buffering=0) as f:
                            while True:
                                b = await r.content.read(CHUNK)
                                if not b:
                                    break
                                    await self._write(f, b, pos)
                                pos += len(b)
                                done = pos - lo
                                progress[k] = done
                                if done % (2 * 1024 * 1024) < len(b) or pos >= hi + 1:
                                    await asyncio.to_thread(self._write_sidecar, path + f'.seg{k}', done)
                                await publish()
                        content_range = r.headers.get('Content-Range', '')
                        m = re.match(r'bytes\s+(\d+)-(\d+)/(\d+|\*)', content_range)
                        if not m:
                            raise RuntimeError('Server returned an invalid Content-Range header')
                        if int(m.group(1)) != request_start or int(m.group(2)) != hi:
                            raise RuntimeError('Server returned an unexpected Range')
                        if m.group(3) != '*' and int(m.group(3)) != total:
                            raise RuntimeError('Server returned an unexpected total size')
                    if done >= hi - lo + 1:
                        try:
                            os.remove(path + f'.seg{k}')
                        except OSError:
                            pass
                        return
                    raise aiohttp.ClientPayloadError('Incomplete range response')
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    await controller.error()
                    if attempt >= retries:
                        raise RuntimeError(f'Segment {k} interrupted after {attempt + 1} attempts: {e}')
                    await update(i, downloaded_size=sum(progress), speed=0, eta=0,
                                 error=f'Segment {k} retrying ({attempt + 1}/{retries})')
                    await self.emit()
                    await asyncio.sleep(delay)
                finally:
                    await controller.leave()

        await asyncio.gather(*(worker(k) for k in range(parts)))
        await controller.tune(True)
        await publish(True)
        await update(i, downloaded_size=total, speed=0, eta=0)

    @staticmethod
    def _ensure_sparse_file(path, total):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'ab') as f:
            f.truncate(total)

    @staticmethod
    def _write_sidecar(path, done):
        tmp = path + '.tmp'
        with open(tmp, 'w', encoding='ascii') as f:
            f.write(str(done))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
