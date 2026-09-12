from api import app,cfg
import uvicorn
if __name__=="__main__":uvicorn.run(app,host=cfg["host"],port=int(cfg["port"]))
