import os
import json
import shutil

from typing import Any, Dict, List

import db_api
import record
import files
from pathlib import Path

DB_ROOT = Path('db_files')


class DBTable(db_api.DBTable):
    def __init__(self, name: str, fields: List[db_api.DBField], key_field_name: str, meta_data=None):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name

        if meta_data is None:
            meta_data = files.load(f"{DB_ROOT}/{self.name}/meta_data.json")

        self.meta_data = meta_data

    def count(self) -> int:
        return self.meta_data["count"]

    def insert_record(self, values: Dict[str, Any]) -> None:
        path = f"{DB_ROOT}/{self.name}"
        data = files.load(f"{path}/{self.meta_data['files_num']}.json")
        index = files.load(f"{path}/{self.meta_data['key']}_index.json")

        if index.get(str(values[self.meta_data["key"]])) is not None:
            raise ValueError()

        if len(data.keys()) >= 1000:
            files.dump({values[self.meta_data["key"]]: values}, f"{path}/ {self.meta_data['files_num'] + 1}.json")
        else:
            data[values[self.meta_data["key"]]] = values
            files.dump(data, f"{path}/{self.meta_data['files_num']}.json")

        record.add(index, values[self.meta_data["key"]], self.meta_data['files_num'])
        files.dump(index, f"{path}/{self.meta_data['key']}_index.json")

        self.meta_data["count"] += 1
        files.dump(self.meta_data, f"{path}/meta_data.json")

    def delete_record(self, key: Any) -> None:
        path = f"{DB_ROOT}/{self.name}"
        index = files.load(f"{path}/{self.meta_data['key']}_index.json")

        if (index.get(str(key)) is None) or len(index[str(key)]) == 0:
            raise ValueError()

        data = files.load(f"{path}/{index[str(key)][0]}.json")
        data.pop(str(key))
        files.dump(data, f"{path}/{index[str(key)]}.json")
        self.meta_data["count"] -= 1
        files.dump(self.meta_data, f"{path}/meta_data.json")
        index.pop(str(key))
        files.dump(index, f"{path}/{self.meta_data['key']}_index.json")

    def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
        path = f"{DB_ROOT}/{self.name}"
        for i in range(1, self.meta_data['files_num'] + 1):
            data = files.load(f"{path}/{i}.json")

            for k in data.keys():
                if record.check(data[k], criteria):
                    self.delete_record(k)

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        path = f"{DB_ROOT}/{self.name}"
        meta_data = files.load(f"{path}/meta_data.json")

        i = record.search_index(f"{DB_ROOT}/{self.name}/{meta_data['key']}_index.json", key)[0]
        data = files.load(f"{path}/{i}.json")

        for k in values.keys():
            data[str(key)][k] = values[k]
        files.dump(data, f"{path}/{i}.json")

    def get_record(self, key: Any) -> Dict[str, Any]:
        path = f"{DB_ROOT}/{self.name}"
        meta_data = files.load(f"{path}/meta_data.json")
        i = record.search_index(f"{path}/{meta_data['key']}_index.json", key)[0]
        data = files.load(f"{path}/{i}.json")

        return data.get(str(key))

    def query_table(self, criteria: List[db_api.SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        path = f"{DB_ROOT}/{self.name}"
        res = {}

        for i in range(1, self.meta_data['files_num'] + 1):
            data = files.load(f"{path}/{i}.json")
            for k in data.keys():
                if record.check(data[k], criteria):
                    res[k] = data[k]

        return res

    def create_index(self, field_to_index: str) -> None:
        path = f"{DB_ROOT}/{self.name}"
        index = {}
        for i in range(1, self.meta_data['files_num'] + 1):
            data = files.load(f"{path}/{i}.json")
            for k in data.keys():
                record.add(index, data[k][field_to_index], i)
        files.dump(index, f"{path}/{field_to_index}_index.json")


class DataBase(db_api.DataBase):
    def create_table(self,
                     table_name: str,
                     fields: List[db_api.DBField],
                     key_field_name: str) -> DBTable:

        fields_names = [f.name for f in fields]
        if key_field_name not in fields_names:
            raise ValueError()
        os.mkdir(f"./{DB_ROOT}/{table_name}")

        meta_data = {"must": fields_names, "key": key_field_name, "count": 0, "files_num": 1}
        with (DB_ROOT / f"{table_name}/meta_data.json").open("w") as the_file:
            json.dump(meta_data, the_file)

        files.dump({}, f"{DB_ROOT}/{table_name}/1.json")

        table = DBTable(table_name, fields, key_field_name, meta_data)
        table.create_index(key_field_name)
        return table

    def num_tables(self) -> int:
        return len(os.listdir(DB_ROOT))

    def get_table(self, table_name: str) -> DBTable:
        meta_data = files.load(f"{DB_ROOT}/{table_name}/meta_data.json")
        fields = [db_api.DBField(name, str) for name in meta_data["must"]]
        return DBTable(table_name, fields, meta_data["key"], meta_data)

    def delete_table(self, table_name: str) -> None:
        shutil.rmtree(f"{DB_ROOT}/{table_name}", ignore_errors=True)

    def get_tables_names(self):
        f = []
        for (dir_path, dir_names, file_names) in os.walk(f"./{DB_ROOT}"):
            f.extend(dir_names)
            break

        return f
