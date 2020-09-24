#!/usr/bin/env python
# coding: utf-8

"""
Einfacher Scatterplot mit Trendlinien-Schar.

Es werden Monatsdurchschnitte für die Temeratur um 12 UTC (also im
Sommer 14 Uhr Ortszeit) ermitelt und folgende lineare Regressionen
gerechnet:
- gesamter Zeitraum
- 11 "alte" Jahre weniger
- 22 "alte" Jahre weniger
- ...
Dabei wird das Interval schrittweise um je 11 Jahre verkürzt, um jeweils
einen vollständigen Schwabe-Zyklus der Sonnenaktivität abzuschneiden.
"""
# Created: 24.09.20

import sqlite3
import logging
import numpy as np
from sklearn.linear_model import LinearRegression

import johanna

from dwdcdc import at2h  # TODO this does not really belong here...
from dwdcdc import const


# transpose [(x,y),...] -> [x,...], [y,...]
def _transpose(rows):
    return [row[0] for row in rows], [row[1] for row in rows]


def _get_station_name(station):
    # assert isinstance(station, int)
    # tup = db.select_single("name from stationen where station = ?", (station, ), cur=cur)
    # return tup[0]
    station = at2h.Station(station)
    return station.description


def fit(x_db, y_db, von, bis):
    # von, bis  - Indexe, keine Jahre, weil von vollständigen Daten augegangen
    #             wird; bis ist EINschließlich

    # https://realpython.com/linear-regression-in-python/#simple-linear-regression-with-scikit-learn
    x = np.array(x_db[von:bis+1]).reshape((-1, 1))
    y = np.array(y_db[von:bis+1])
    model = LinearRegression().fit(x, y)
    logging.info("von %d bis %d: dT = %0.3f" % (x_db[von], x_db[bis], model.coef_))
    x_pred = np.array([x_db[von], x_db[bis]]).reshape((-1, 1)) # nur die Enden
    y_pred = model.predict(x_pred)
    return x_pred, y_pred, model.coef_


def fit_all(x_db, y_db):
    # Anfnag: 11er-Schritte, letzten Wert auslassen
    # Ende: nicht ausgerechnet 2019
    base = [i for i in range(0, len(x_db), 11)][:-1]
    return [ fit(x_db, y_db, von=von, bis=len(x_db)-1) for von in base]


def plot(plt, station: int = const.MANNHEIM,
         monat: int = 6, stunde: int = 12,
         von: int = 0, bis: int = 3000) -> None:
    """

    :param plt: wird vom jupyter notebook bereitgestellt:
            from matplotlib import pyplot as plt
            %matplotlib inline # <- deswegen!
    :param station: numerischer Stations-Schlüssel
    :param monat: 1 = Januar,...
    :param stunde: 0..23
    :param von: Jahre (4-stell.)
    :param bis: Jahre (4-stell.)
    """
    with johanna.Timer() as overall:
        name = _get_station_name(station)

        with johanna.Timer() as timer:
            with johanna.Connection("select readings") as c:
                c.cur.execute('''
                    select year, avg(temp) val
                        from readings
                        where station = ?
                          and month = ?
                          and hour = ?
                          and year between ? and ?
                        group by year
                        order by year asc
                ''', (station, monat, stunde, von, bis))
                rows = [row for row in c.cur]
            x_db, y_db = _transpose(rows)
        logging.info(f"Select: {timer.read()}")

        # https://realpython.com/linear-regression-in-python/#simple-linear-regression-with-scikit-learn
        x = np.array(x_db).reshape((-1, 1))
        y = np.array(y_db)

        with johanna.Timer() as timer:
            trends = fit_all(x_db, y_db)
        logging.info(f"LinearRegression (all): {timer.read()}")

        # https://towardsdatascience.com/linear-regression-using-python-b136c91bf0a2
        plt.rc('figure', figsize=(20.0, 10.0))
        plt.scatter(x, y, s=10, color='green', label="Einzelwerte")
        plt.xlabel('Jahr')
        plt.ylabel('Mitteltemperatur %d Uhr (UTC), %s, %s' % (stunde, const.monat_as_string(monat), name))
        # len(trends)-1 -> 1.0
        # 0 -> 0.1
        # c = a * idx + b
        #       b = 0.1
        #       a = ( 1.0 - 0.1 ) / ( len(trends) - 1 )
        a = ( 1.0 - 0.1 ) / ( len(trends) - 1 )
        b = 0.1
        for idx, trend in enumerate(trends):
            color = "%0.2f" % ( 1.0 - (a * idx + b) ,)
            plt.plot(trend[0], trend[1], color=color)
        # https://matplotlib.org/api/pyplot_api.html#matplotlib.pyplot.legend
        plt.legend(loc=4)
        plt.show()

    logging.info(f"Overall: {overall.read()}")
