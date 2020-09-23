#!/usr/bin/env python
# coding: utf-8

"""
Convenience API to package data
Following wim's advice in https://stackoverflow.com/a/58941536/3991164

Created: 09.05.20
"""

import logging
import pkgutil
import json as _thejson

_DATA_BUFFER = dict()


def get(name):
    if not name in _DATA_BUFFER:
        logging.info(f'pkgutil.get_data({__name__}, "data/{name}")')
        binary = pkgutil.get_data(__name__, f"data/{name}")
        _DATA_BUFFER[name] = binary
    return _DATA_BUFFER[name]


def text(name):
    return get(name).decode("utf-8")


def json(name):
    return _thejson.loads(text(name))


# DONE buffering
def feeds():
    return json("feeds.json")


def schema(name):
    return text(name + ".sql")
