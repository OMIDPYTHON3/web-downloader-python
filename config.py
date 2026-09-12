import json,os
CONFIG_FILE=os.getenv('WD_CONFIG','config.json')
DEFAULT={'host':'0.0.0.0','port':8585,'download_dir':'./downloads','download_roots':['./downloads','/mnt/hdd2','/home/games'],
'max_concurrent':3,'default_threads':8,'retry_count':5,'retry_delay':3,'auto_resume':True,'global_speed_limit':0}
def load():
    c=DEFAULT.copy()
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE,encoding='utf8') as f:c.update(json.load(f))
        except Exception: pass
    c['download_dir']=os.path.abspath(os.path.expanduser(c['download_dir']))
    c['download_roots']=list(dict.fromkeys([os.path.abspath(os.path.expanduser(str(v))) for v in c.get('download_roots',[])] + [c['download_dir']]))
    os.makedirs(c['download_dir'],exist_ok=True)
    for p in c['download_roots']:
        try: os.makedirs(p,exist_ok=True)
        except: pass
    return c
def save(c):
    with open(CONFIG_FILE,'w',encoding='utf8') as f:json.dump(c,f,indent=2)
