from dataclasses import dataclass

from lib.hconn.conn import HConn, BaseModel

client = HConn('','','')


@dataclass()
class Ligand(BaseModel):
    name: str = ''
    score: float = 0.0
    rank: int = 0
    l: list = list
    d: dict = dict
    ins: str = ''
