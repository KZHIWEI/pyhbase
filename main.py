import datetime
import struct
from dataclasses import dataclass
from datetime import time

from lib.hconn.conn import HConn, SplitKeyOption, BaseModel

client = HConn('root', 'root', '')


@dataclass(kw_only=True)
class Ligand(BaseModel):
    # MOLBLOCK: object = list
    name: str = ''
    score: float = 0.0
    rank: int = 0
    l: list = list
    d: dict = dict


l1 = Ligand(id=3, name='1name\\n', score=0.564, rank=56, l=[1, 2, 3], d={'a': 'b', 'c': 'd'})
l2 = Ligand(id=4, name='2name\\n', score=-21.41856, rank=-5, l=[4, 5, 6], d={'d': 'e', 'g': 't'})
client.puts_thread('wangzhen', 'test', 10, [l1, l2])
now = datetime.datetime.now()
# print(len(client.scan('wangzhen', 'test', 10, 0, 5)))
res = client.gets('wangzhen', 'test', 10, [3, 4],model=Ligand)
print(datetime.datetime.now() - now)
print(res)
# print([l1,l2])
# ids = [6622636]
# result = client.gets('ligand', 'data', 8, ids, pluck=['MOLBLOCK'], model=Ligand)
# print(result)
