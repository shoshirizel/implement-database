import json

from typing import Any, Dict, List, Type
import db_api
from pathlib import Path

DB_ROOT = Path('db_files')


class DBTable(db_api.DBTable):
    def count(self) -> int:
        with open(f"{DB_ROOT}/{self.name}/meta_data.json") as the_file:
            meta_data = json.load(the_file)

        return meta_data["count"]

    def insert_record(self, values: Dict[str, Any]) -> None:
        with open(f"{DB_ROOT}/{self.name}/meta_data.json") as the_file:
            meta_data = json.load(the_file)

        with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "w") as the_file:
            data = json.load(the_file)
            data[values[meta_data["key"]]] = values
            json.dump(data, the_file)

    def delete_record(self, key: Any) -> None:
        with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "w") as the_file:
            data = json.load(the_file)
            if data.get(key):
                data.pop(key)
                with open(f"{DB_ROOT}/{self.name}/meta_data.json", "w") as meta_data_file:
                    meta_data = json.load(the_file)
                    meta_data["count"] -= 1
                    json.dump(meta_data, meta_data_file)

            json.dump(data, the_file)
    #
    # def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
    #     with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "w") as the_file:
    #         data = json.load(the_file)
    #         for record in criteria:

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "w") as the_file:
            data = json.load(the_file)
            if data.get(key):
                for k in values.keys():
                    data[key][k] = values[k]

            json.dump(data, the_file)



