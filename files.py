import json


def load(path):
    with open(path, "r") as the_file:
        return json.load(the_file)


def dump(data, path):
    with open(path, "w") as the_file:
        json.dump(data, the_file)