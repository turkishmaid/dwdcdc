#!/usr/bin/env python
# coding: utf-8

"""
Provide some helpful constants.

Created: 24.09.20
"""


MONATE = ["Januar", "Februar", "März", "April", "Mai", "Juni",
          "Juli", "August", "September", "Oktober", "November", "Dezember"]

def monat_as_string(nr: int) -> str:
    # 1 -> Januar
    assert isinstance(nr, int)
    assert 1 <= nr <= 12
    return MONATE[nr - 1]


# we will see whether these are the same for all datasets

MANNHEIM = 5906
POTSDAM = 3987
GEISENHEIM = 1580
WAGHAEUSEL = 5275
PHILIPPSBURG = 6243
SINSHEIM = 4719
BROCKEN = 722
ZUGSPITZE = 5792
DRESDEN = 1048
HOHENPEISSENBERG = 2290
