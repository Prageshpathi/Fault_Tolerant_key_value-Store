import json
import os
import threading
from typing import Dict,Optional

class keyValueStore:

    def __init__(self,wal_path:str="data.wal") -> None:
        self.wal_path=wal_path
        self._store: Dict[str,str]={}
        self._lock= threading.Lock()
        self._replay_wal()

    def _replay_wal(self) -> None:
        if not os.path.exists(self.wal_path):
            return
        valid_lines=[]
        with open(self.wal_path,"r",encoding="utf-8") as f:
            for line in f:
                stripped=line.strip()
                if not stripped:
                    continue
                try:
                    entry=json.loads(stripped)
                    op=entry["op"]
                    key=entry["key"]
                    if op=="PUT":
                        self._store[key]=entry["val"]
                    elif op=="DEL":
                        self._store.pop(key,None)
                    valid_lines.append*(line) 
                except(json.JSONDecodeError,KeyError):
                    break
        with open(self.wal_path,"w",encoding="utf-8") as f:
            f.writelines(valid_lines)
            f.flush()
            os.fsync(f.fileno())
    def _append_log(self,payload:dict) -> None:
        record =json.dumps(payload) + "\n"
        with open(self.wal_path,"a",encoding="utf-8") as f:
            f.write(record)
            f.flush()
            os.fsync(f.fileno())

    def put(self,key:str,val:str) -> None:
        with self._lock:
            self._append_log({"op":"PUT","key":key,"val":val}) 
            self._store[key]= val
    def get(self,key:str) -> None:
        with self._lock:
            return self._store.get(key)
    def delete(self,key:str) -> None:
        with self._lock:
            self._append_log({"op":"DEL","key":key})
            self._store.pop(key,None)
    def snapshot(self)-> Dict[str,str]:
        with self._lock:
            return dict(self._store)                            