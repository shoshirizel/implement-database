from typing import List
import operator as op

import db_api


def check(record, criteria: List[db_api.SelectionCriteria]):
    ops = {'>': op.gt, '<': op.lt, '=': op.eq}
    for c in criteria:
        if record.get(c.field_name) is None:
            return False
        if not ops[c.operator](record[c.field_name], c.value):
            return False
    return True
