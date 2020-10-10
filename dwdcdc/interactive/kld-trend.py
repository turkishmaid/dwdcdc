#!/usr/bin/env python
# coding: utf-8



"""
Einfacher Scatterplot mit Trendlinie.

"""
# Created: 24.09.20


import logging
from sklearn.linear_model import LinearRegression
import numpy as np

import johanna

from dwdcdc.stations import Station
from dwdcdc import const


# from dwdutil import db, monate, timer
# from dwdmaster import stationsids


# transpose [(x,y),...] -> [x,...], [y,...]
def _transpose(rows):
    return [row[0] for row in rows], [row[1] for row in rows]


def _get_station_name(station):
    # assert isinstance(station, int)
    # with johanna.Connection("Stationsname") as c:
    #     c.cur.select_single("select name from stationen where station = ?", (station, ))
    #     rows = [row for row in c.cur]
    # return rows[0][0]
    station = Station(station)
    return station.description


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
            model = LinearRegression().fit(x, y)
            logging.info(f"dT p.a.: {model.coef_}")
            x_pred = np.array([x_db[0], x_db[-1]]).reshape((-1, 1)) # nur die Enden
            y_pred = model.predict(x_pred)
        logging.info(f"LinearRegression: {timer.read()}")

        # https://towardsdatascience.com/linear-regression-using-python-b136c91bf0a2
        plt.rc('figure', figsize=(20.0, 10.0))
        plt.scatter(x, y, s=10, color='green', label="Einzelwerte")
        plt.xlabel('Jahr')
        plt.ylabel('Mitteltemperatur %d Uhr (UTC), %s, %s' % (stunde, const.monat_as_string(monat), name))
        plt.plot(x_pred, y_pred, color='red', label="Trend")
        # https://matplotlib.org/api/pyplot_api.html#matplotlib.pyplot.legend
        plt.legend(loc=4)
        plt.show()

    logging.info(f"Overall: {overall.read()}")
