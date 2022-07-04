# pyhbase

## Install
```shell
pip install git+https://github.com/KZHIWEI/pyhbase.git
```
## Connect To DataBase

```python
from lib.hconn.conn import HConn

client = HConn('username', 'password', 'address')
```

## Declaring Models

```python
from dataclasses import dataclass
from lib.hconn.conn import BaseModel

@dataclass()
class Student(BaseModel):
    name: str = ''
    score: float = 0.0
    age: int = 0
    teacher_id: list = list
    d: dict = dict
```

Models are must have `@database` decorator and extend `BaseModel`, for each primitive field, default value is required, 
and for each complex type, default value is list or dict, technically list and dict follows same behavior.