#!/usr/bin/env python
# coding: utf-8

"""
General tooling tailored for DWD data.
Expected to fit for all datasets (may need refactoring over time).

Created: 25.09.20
"""

def dwdts2iso(s: str) -> str:
    """
    Formats a CDC timestamp as ISO, e.g. 19690523 -> 1969-05-23
    :param s: Timestamp like YYYYMMDD or YYYYMMDDHH
    :return: ISO Representation like YYYY-MM-DD or YYYY-MM-DD HH:00:00
    """
    if len(s) == 8:
        # 19690523 -> 1969-05-23
        return "%s-%s-%s" % (s[0:4], s[4:6], s[6:])
    elif len(s) == 10:
        # 1969052307 -> 1969-05-23 07:00:00 (not clear what time would be the "right" one,
        #   so go for one that preserves the day)
        return "%s-%s-%s %s:00:00" % (s[0:4], s[4:6], s[6:8], s[8:10])


# frei nach https://www.giga.de/ratgeber/specials/abkuerzungen-der-bundeslaender-in-deutschland-tabelle/
LAND_MAP = {
    "Baden-Württemberg": "BaWü",  # BW
    "Bayern": "BY",
    "Berlin": "BER",  # BE
    "Brandenburg": "BB",
    "Bremen": "HB",
    "Hamburg": "HH",
    "Hessen": "HE",
    "Mecklenburg-Vorpommern": "MV",
    "Niedersachsen": "NDS",
    "Nordrhein-Westfalen": "NRW",  # NW
    "Rheinland-Pfalz": "RLP",  # RP
    "Saarland": "SL",
    "Sachsen": "SN",
    "Sachsen-Anhalt": "ST",
    "Schleswig-Holstein": "SH",
    "Thüringen": "TH"
}

def dwdland2short(dwdland: str) -> str:
    if dwdland in LAND_MAP:
        return LAND_MAP[dwdland]
    else:
        return "?"


def station_from_fnam(fnam: str) -> int:
    return int(fnam.split(".")[0].split("_")[2])

