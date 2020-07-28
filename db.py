import os
import json
import shutil

from typing import Any, Dict, List

import db_api
from pathlib import Path

import record

DB_ROOT = Path('db_files')


class DBTable(db_api.DBTable):
    def count(self) -> int:
        with open(f"{DB_ROOT}/{self.name}/meta_data.json") as the_file:
            meta_data = json.load(the_file)

        return meta_data["count"]

    def insert_record(self, values: Dict[str, Any]) -> None:
        with open(f"{DB_ROOT}/{self.name}/meta_data.json") as the_file:
            meta_data = json.load(the_file)

        with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "r") as the_file:
            data = json.load(the_file)

        if data.get(str(values[meta_data["key"]])) is not None:
            raise ValueError()

        with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "w") as the_file:
            data[values[meta_data["key"]]] = values
            json.dump(data, the_file)

        meta_data["count"] += 1
        with open(f"{DB_ROOT}/{self.name}/meta_data.json", "w") as the_file:
            json.dump(meta_data, the_file)

    def delete_record(self, key: Any) -> None:
        with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "r") as the_file:
            data = json.load(the_file)

        if data.get(str(key)):
            with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "w") as the_file:
                data.pop(str(key))
                with open(f"{DB_ROOT}/{self.name}/meta_data.json", "r") as meta_data_file:
                    meta_data = json.load(meta_data_file)
                    meta_data["count"] -= 1

                with open(f"{DB_ROOT}/{self.name}/meta_data.json", "w") as meta_data_file:
                    json.dump(meta_data, meta_data_file)

                json.dump(data, the_file)

        else:
            raise ValueError()

    def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
        with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "r") as the_file:
            data = json.load(the_file)

        for k in data.keys():
            if record.check(data[k], criteria):
                self.delete_record(k)

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "r") as the_file:
            data = json.load(the_file)

        if data.get(str(key)):
            with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "w") as the_file:
                for k in values.keys():
                    data[str(key)][k] = values[k]

                json.dump(data, the_file)

    def get_record(self, key: Any) -> Dict[str, Any]:
        with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "r") as the_file:
            data = json.load(the_file)

        return data.get(str(key))

    def query_table(self, criteria: List[db_api.SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        res = {}
        with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "r") as the_file:
            data = json.load(the_file)
        for k in data.keys():
            if record.check(data[k], criteria):
                res[k] = data[k]

        return res


class DataBase(db_api.DataBase):
    def create_table(self,
                     table_name: str,
                     fields: List[db_api.DBField],
                     key_field_name: str) -> DBTable:

        li = [f.name for f in fields]
        if key_field_name not in li:
            raise ValueError()
        os.mkdir(f"./{DB_ROOT}/{table_name}")

        with (DB_ROOT / f"{table_name}/meta_data.json").open("w") as the_file:
            json.dump({"must": li, "key": key_field_name, "count": 0}, the_file)

        with open(f"{DB_ROOT}/{table_name}/{table_name}.json", "w") as the_file:
            json.dump({}, the_file)

        return DBTable(table_name, {"must": li, "key": key_field_name, "count": 0}, key_field_name)

    def num_tables(self) -> int:
        return len(os.listdir(DB_ROOT))

    def get_table(self, table_name: str) -> DBTable:
        with open(f"{DB_ROOT}/{table_name}/meta_data.json") as the_file:
            meta_data = json.load(the_file)

        return DBTable(table_name, meta_data, "h")

    def delete_table(self, table_name: str) -> None:
        shutil.rmtree(f"{DB_ROOT}/{table_name}", ignore_errors=True)

    def get_tables_names(self):
        f = []
        for (dir_path, dir_names, file_names) in os.walk(f"./{DB_ROOT}"):
            f.extend(dir_names)
            break

        return f

# root = DataBase()

# print(root.get_tables_names())
# table = root.create_table("s", [db_api.DBField("id", int)], "id")
# table.insert_record({"id": 123, "name": "bh"})
# table.insert_record({"id": 124, "name": "bhll"})
# print(table.count())
# table.delete_record(124)
# print(table.count())
# table.update_record(123, {"name": "dfg", "age": 90})
# print(table.get_record(123))
