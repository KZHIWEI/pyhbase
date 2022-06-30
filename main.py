import datetime
import struct
from dataclasses import dataclass
from datetime import time

from lib.hconn.conn import HConn, SplitKeyOption, BaseModel

client = HConn('user', 'password', 'host')


@dataclass(kw_only=True)
class Ligand(BaseModel):
    name: str = ''
    score: float = 0.0
    rank: int = 0


l1 = Ligand(id=3, name='1name', score=0.564, rank=56)
l2 = Ligand(id=4, name='2name', score=-21.41856, rank=-5)
client.puts_thread('wangzhen', 'test', 10, [l1, l2])
now = datetime.datetime.now()
print(len(client.scan('wangzhen', 'test', 10, 0, 5)))
print(datetime.datetime.now() - now)
