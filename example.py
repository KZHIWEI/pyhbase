from dataclasses import dataclass

from lib.hconn.conn import HConn, BaseModel, SplitKeyOption

client = HConn('root', 'root', '')

namespace = 'zzw'

table_name = 'test'


@dataclass()
class Ligand(BaseModel):
    SDF: str = ''
    MOLBLOCK: list = list
    PDBQT: list = list


# client.create_namespace(namespace)
client.create_table(namespace, table_name, split_option=SplitKeyOption(min_id=0, max_id=100, split_size=100))
l1 = Ligand(id=1, SDF='i am sdf', MOLBLOCK=['1', '23', '45'], PDBQT=['i', 'am', 'pdbqt'])
l2 = Ligand(id=2, SDF='you are sdf', MOLBLOCK=['32', '1', '1236'], PDBQT=['you', 'are', 'pdbqt'])
client.puts(namespace, table_name, 8, datas=[l1, l2])

res = client.gets(namespace, table_name, 8, [1, 2], model=Ligand)

print(res)
# [Ligand(id=1, SDF='i am sdf', MOLBLOCK=['1', '23', '45'], PDBQT=['i', 'am', 'pdbqt']), Ligand(id=2, SDF='you are sdf', MOLBLOCK=['32', '1', '1236'], PDBQT=['you', 'are', 'pdbqt'])]
