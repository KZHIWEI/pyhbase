import dataclasses
import math
import struct
from concurrent.futures import ThreadPoolExecutor
from typing import List, Literal, Dict, Type

from thrift.protocol import TBinaryProtocol
from thrift.transport import THttpClient

from lib.hbase import THBaseService
from lib.hbase.ttypes import TNamespaceDescriptor, TTableName, TTableDescriptor, TColumnFamilyDescriptor, TPut, \
    TColumnValue, TGet, TColumn, TDelete, TScan
from lib.hconn.pool import ConnectionPool
from dataclasses import dataclass


def _format_id_with_length(id, length):
    str_length = '0' + str(length)
    return f'{id:{str_length}}'


def compute_max_length(max_id):
    return len(str(max_id))


def _format_bytes_to_id(id_bytes: bytes):
    return int(id_bytes.decode('utf8'))


@dataclass
class SplitKeyOption:
    min_id: int
    max_id: int
    split_size: int

    def split_key(self):
        result = []
        str_length = len(str(self.max_id))
        for i in range(self.min_id, self.max_id, self.split_size):
            result.append(_format_id_with_length(i, str_length))
        return result


class InvalidInstance(Exception):
    pass


class UnsupportedType(Exception):
    pass


@dataclass(kw_only=True)
class BaseModel:
    id: int = 0

    @classmethod
    def from_instance(cls, instance):
        return cls(**dataclasses.asdict(instance))


class HConn:
    def __init__(self, user: str, password: str, address: str, int_length: int = 64,
                 int_byteorder: Literal['big', 'little'] = 'big', max_usage=10, max_size=20):
        self._user = user
        self._family = 'f'.encode('utf8')
        self._password = password
        self._address = address
        self._int_length = int_length
        self._int_byteorder = int_byteorder
        self._pool = ConnectionPool(create=self._open, close=lambda conn: conn[1].close(), max_usage=max_usage,
                                    max_size=max_size)

    def _open(self):
        url = self._address
        transport = THttpClient.THttpClient(url)
        headers = {"ACCESSKEYID": self._user, "ACCESSSIGNATURE": self._password}
        transport.setCustomHeaders(headers)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
        client = THBaseService.Client(protocol)
        transport.open()
        return client, transport

    def list_namespace(self):
        with self._pool.item() as (client, transport):
            result = []
            for each in client.listNamespaceDescriptors():
                result.append(each.name)
            return result

    def create_namespace(self, name):
        with self._pool.item() as (client, transport):
            return client.createNamespace(TNamespaceDescriptor(name=name))

    def _tableName(self, namespace, table_name):
        return TTableName(ns=namespace, qualifier=table_name)

    def create_table(self, namespace, name, split_option=None):
        with self._pool.item() as (client, transport):
            table_name = self._tableName(namespace, name)
            client.createTable(
                TTableDescriptor(tableName=table_name,
                                 columns=[TColumnFamilyDescriptor(name="f")]
                                 ),
                None if not split_option else split_option.split_key()
            )

    def _dataclass_to_tput(self, row_key_length: int, datas: List[BaseModel]):
        value_to_put = []
        for data in datas:
            if isinstance(data, dict) and 'id' in data:
                value_to_put.append(data)
            elif isinstance(data, BaseModel):
                temp = {}
                for field in dataclasses.fields(data):
                    key, value = field.name, getattr(data, field.name)
                    temp[key] = value
                value_to_put.append(temp)
            else:
                raise InvalidInstance
        puts = []
        for each in value_to_put:
            values = []
            for each_key, each_value in each.items():
                if each_key == 'id':
                    continue
                values.append(TColumnValue(
                    family=self._family,
                    qualifier=each_key.encode('utf8'),
                    value=self._convert_value_to_bytes(each_value)))
            puts.append(TPut(
                row=self._convert_value_to_bytes(_format_id_with_length(each['id'], row_key_length)),
                columnValues=values
            ))
        return puts

    def _dataclass_to_tget(self, row_key_length: int, ids: List[int], pluck: List[str] = None):
        if pluck is None:
            pluck = []
        tgets = []
        for each in ids:
            id_str = _format_id_with_length(each, row_key_length)
            columns = None
            if pluck:
                columns = []
                for each_pluck in pluck:
                    columns.append(TColumn(family=self._family,
                                           qualifier=self._convert_value_to_bytes(each_pluck))
                                   )
            tgets.append(TGet(
                row=self._convert_value_to_bytes(_format_id_with_length(each, row_key_length)),
                columns=columns
            ))
        return tgets

    def _convert_value_to_bytes(self, value):
        if isinstance(value, str):
            return value.encode('utf8')
        elif isinstance(value, int):
            return value.to_bytes(self._int_length, self._int_byteorder, signed=True)
        elif isinstance(value, float):
            return struct.pack('d', value)
        elif isinstance(value, bytes):
            return value
        else:
            raise UnsupportedType

    def _convert_bytes_to_value(self, bytes_value: bytes, value_type):
        if not isinstance(bytes_value, bytes):
            return bytes_value
        if value_type is str:
            return bytes_value.decode()
        elif value_type is int:
            return int.from_bytes(bytes_value, self._int_byteorder, signed=True)
        elif value_type is float:
            return struct.unpack('d', bytes_value)[0]
        elif value_type is bytes:
            return value_type
        else:
            raise UnsupportedType

    def _table_in_bytes(self, namespace, name):
        return f"{namespace}:{name}".encode("utf8")

    def puts(self, namespace: str, name: str, row_key_length: int, datas: List[BaseModel]):
        tputs = self._dataclass_to_tput(row_key_length, datas)
        with self._pool.item() as (client, transport):
            client.putMultiple(table=self._table_in_bytes(namespace, name), tputs=tputs)

    def _set_attr(self, obj, attr, value):
        if isinstance(obj, dict):
            obj[attr] = value
            return
        elif hasattr(obj, attr):
            converted_value = self._convert_bytes_to_value(value, type(getattr(obj, attr)))
            setattr(obj, attr, converted_value)
            return
        else:
            return

    def gets(self, namespace: str, name: str, row_key_length: int, ids: List[int],
             pluck: List[str] = None, model: Type = None):
        tgets = self._dataclass_to_tget(row_key_length, ids, pluck=pluck)
        t_result = None
        result = []
        with self._pool.item() as (client, transport):
            t_result = client.getMultiple(table=self._table_in_bytes(namespace, name), tgets=tgets)
        for each in t_result:
            temp = None
            if model is not None and issubclass(model, BaseModel):
                temp = model()
            else:
                temp = {}
            if not each.row:
                continue
            self._set_attr(temp, 'id', _format_bytes_to_id(each.row))
            result.append(temp)
            for each_columns in each.columnValues:
                self._set_attr(temp,
                               self._convert_bytes_to_value(each_columns.qualifier, str),
                               each_columns.value
                               )
        return result

    def deletes(self, namespace: str, name: str, row_key_length: int, ids: List[int]):
        deletes = []
        for each in ids:
            deletes.append(TDelete(
                row=_format_id_with_length(each, row_key_length)
            ))
        with self._pool.item() as (client, transport):
            client.deleteMultiple(table=self._table_in_bytes(namespace, name), tdeletes=deletes)

    def scan(self, namespace: str, name: str, row_key_length: int, start_id: int, end_id: int, batch_size: int = 100,
             pluck: List[str] = None, model: Type = None):
        start_row = _format_id_with_length(start_id, row_key_length)
        stop_row = _format_id_with_length(end_id, row_key_length)
        columns = None
        t_results = []
        if pluck:
            columns = []
            for each in pluck:
                columns.append(TColumn(
                    family=self._family,
                    qualifier=each.encode("utf8")
                ))
        scan = TScan(startRow=start_row, stopRow=stop_row, columns=columns)

        def create_closest_row_after(row):
            array = bytearray(row)
            array.append(0x00)
            return bytes(array)

        with self._pool.item() as (client, transport):
            while True:
                last_result = None
                # getScannerResults会自动完成open,close 等scanner操作，HBase增强版必须使用此方法进行范围扫描
                current_results = client.getScannerResults(self._table_in_bytes(namespace, name), scan, batch_size)
                for result in current_results:
                    t_results.append(result)
                    last_result = result
                # 如果一行都没有扫描出来，说明扫描已经结束，我们已经获得startRow和stopRow之间所有的result
                if last_result is None:
                    break
                # 如果此次扫描是有结果的，我们必须构造一个比当前最后一个result的行大的最小row，继续进行扫描，以便返回所有结果
                else:
                    next_start_row = create_closest_row_after(last_result.row)
                    scan = TScan(startRow=next_start_row, stopRow=stop_row, caching=batch_size, columns=columns)
        result = []
        for each in t_results:
            temp = None
            if model is not None and issubclass(model, BaseModel):
                temp = model()
            else:
                temp = {}
            self._set_attr(temp, 'id', _format_bytes_to_id(each.row))
            result.append(temp)
            for each_columns in each.columnValues:
                self._set_attr(temp,
                               self._convert_bytes_to_value(each_columns.qualifier, str),
                               each_columns.value)
        return result

    def gets_thread(self, namespace: str, name: str, row_key_length: int, ids: List[int],
                    pluck: List[str] = None, model: Type = None, batch_size=500, max_workers=10):
        futures = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i in range(0, len(ids), batch_size):
                sub = executor.submit(self.gets,
                                      namespace=namespace,
                                      name=name,
                                      row_key_length=row_key_length,
                                      ids=ids[i:i + batch_size],
                                      pluck=pluck, model=model)
                futures.append(sub)
        result = []
        for each in futures:
            result.extend(each.result())
        return result

    def puts_thread(self, namespace: str, name: str, row_key_length: int, datas: List[BaseModel], batch_size=50, max_workers=10):
        futures = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i in range(0, len(datas), batch_size):
                sub = executor.submit(self.puts,
                                      namespace=namespace,
                                      name=name,
                                      row_key_length=row_key_length,
                                      datas=datas[i:i + batch_size])
                futures.append(sub)
        for each in futures:
            each.result()
