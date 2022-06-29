import datetime
import struct
from dataclasses import dataclass
from datetime import time

from lib.hconn.conn import HConn, SplitKeyOption, BaseModel

client = HConn('root', 'root', 'http://ld-8vb09t9718u69w60k-proxy-lindorm-pub.lindorm.rds.aliyuncs.com:9190')


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
# result = client.gets_thread('ligand', 'data', 8, ids, pluck=['MOLBLOCK'])
print(datetime.datetime.now() - now)
# print(client.scan('wangzhen', 'test', 10, 0, 3, model=Ligand))
# client.deletes('wangzhen', 'test', 10, [1, 2])
# a = 5
# res = a.to_bytes(64, 'big', signed=True)
# print(res)
# print(int.from_bytes(res, 'big', signed=True))
#
# f = 22.055232121323211232131
# res = struct.pack('d', f)
# print(res)
# print(struct.unpack('d', res)[0])
