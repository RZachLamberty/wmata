#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module: data.py
Author: zlamberty
Created: 2016-12-16

Description:
    acquire data from the metro via its open api

Usage:
    <usage>

"""

import argparse
import datetime
import os
import requests
import time

import psycopg2
import yaml

import eri.logging as logging


# ----------------------------- #
#   Module Constants            #
# ----------------------------- #

URL_BASE = 'https://api.wmata.com/'
URLS = {
    'TrainPositions': 'TrainPositions/TrainPositions',
    'StandardRoutes': 'TrainPositions/StandardRoutes',
    'Lines': 'Rail.svc/json/jLines',
    'StationInformation': 'Rail.svc/json/jStations',
    'StationToStationInformation': 'Rail.svc/json/jSrcStationToDstStationInfo',
}
URLS = {k: '{}{}'.format(URL_BASE, v) for (k, v) in URLS.items()}

INSERT_SQL = {}
INSERT_SQL['TrainPositions'] = """INSERT INTO
    train_positions
VALUES (
    %(CarCount)s, %(CircuitId)s, %(DestinationStationCode)s, %(DirectionNum)s,
    %(LineCode)s, %(SecondsAtLocation)s, %(ServiceType)s, %(TrainId)s,
    %(TimeStamp)s
)
"""
INSERT_SQL['StandardRoutes'] = """INSERT INTO
    standard_routes
VALUES (
    %(LineCode)s, %(CircuitId)s, %(SeqNum)s, %(StationCode)s, %(TrackNum)s,
    %(TimeStamp)s
)
"""
INSERT_SQL['Lines'] = """INSERT INTO
    lines
VALUES (
    %(DisplayName)s, %(EndStationCode)s, %(InternalDestination1)s,
    %(InternalDestination2)s, %(LineCode)s, %(StartStationCode)s, %(TimeStamp)s
)
"""
INSERT_SQL['StationInformation'] = """INSERT INTO
    station_information
VALUES (
    %(City)s, %(Code)s, %(Lat)s, %(LineCode1)s, %(LineCode2)s, %(LineCode3)s,
    %(LineCode4)s, %(Lon)s, %(Name)s, %(State)s, %(StationTogether1)s,
    %(StationTogether2)s, %(Street)s, %(Zip)s, %(TimeStamp)s
)
"""
INSERT_SQL['StationToStationInformation'] = """INSERT INTO
    station_to_station_information
VALUES (
    %(CompositeMiles)s, %(DestinationStation)s, %(OffPeakTime)s, %(PeakTime)s,
    %(RailTime)s, %(SeniorDisabled)s, %(SourceStation)s, %(TimeStamp)s
)
"""

logger = logging.getLogger(__name__)
logging.configure()


# ----------------------------- #
#   Main routine                #
# ----------------------------- #

class WmataParseError(Exception):
    pass


class WmataScraper(object):
    """interface for objects which will scrape wmata api urls and persist
    responses to the database in some way

    """
    def __init__(self, api_key, url, contentType='json'):
        self.params = {'contentType': contentType}
        self.headers = {'api_key': api_key}
        self.url = url

    def get(self, *args, **kwargs):
        raise NotImplementedError

    def publish(self, *args, **kwargs):
        raise NotImplementedError


class PostgresPublisher(object):
    """mixin for publishing to local postgres database"""
    def __init__(self, dsnargs=None):
        """all arguments are passed through *directly* to a psycopg2 connect
        function, so see that function for full documentation. dsnargs should be
        a dictionary

        """
        if dsnargs is None:
            raise WmataParseError(
                "you must supply either a connection string (pgargs) or "
                "connection parameters (pgkwargs)"
            )
        self.dsnargs = dsnargs

    def connect(self):
        return psycopg2.connect(**self.dsnargs)


class TrainPositions(WmataScraper, PostgresPublisher):
    """the live of position trains is exposed by a single url, with data updated
    approximately every 10 seconds. This object is dumb -- simple query, simple
    persistence (postgres assumed)

    """
    def __init__(self, api_key, url=URLS['TrainPositions'], contenttype='json',
                 dsnargs=None):
        WmataScraper.__init__(
            self, api_key=api_key, url=url, contentType=contentType
        )
        PostgresPublisher.__init__(self, dsnargs)

    def get(self):
        """simple wrapper to the wmata api train positions end point

        args:
            None

        returns:
            tp (dict): requests library parsed json response from api

        raises:
            standard requests library errors

        """
        now = datetime.datetime.now()
        j = requests.get(
            url=self.url,
            params=self.params,
            headers=self.headers
        ).json()
        for row in j['TrainPositions']:
            row['TimeStamp'] = now
        return j

    def publish(self, j):
        """publish a given api response to postgres"""
        with self.connect() as con:
            with con.cursor() as cur:
                cur.executemany(
                    INSERT_SQL['TrainPositions'], j['TrainPositions']
                )


class StandardRoutes(WmataScraper, PostgresPublisher):
    """we only need to do this once, basically. This information is changed
    infrequently (per the wmata api docs, at least)

    """
    def __init__(self, api_key, url=URLS['StandardRoutes'], contentType='json',
                 dsnargs=None):
        WmataScraper.__init__(
            self, api_key=api_key, url=url, contentType=contentType
        )
        PostgresPublisher.__init__(self, dsnargs)

    def get(self):
        """simple wrapper to the wmata api standard routes end point

        args:
            None

        returns:
            tp (dict): requests library parsed json response from api

        raises:
            standard requests library errors

        """
        now = datetime.datetime.now()
        j = requests.get(
            url=self.url,
            params=self.params,
            headers=self.headers
        ).json()
        # yes we're denormalizing no I don't care no you shut up
        jout = {'StandardRoutes': []}
        for revline in j['StandardRoutes']:
            for circuitdict in revline['TrackCircuits']:
                x = circuitdict.copy()
                x['LineCode'] = revline['LineCode']
                x['TrackNum'] = revline['TrackNum']
                x['TimeStamp'] = now
                jout['StandardRoutes'].append(x)
        return jout

    def publish(self, j):
        """publish a given api response to postgres"""
        with self.connect() as con:
            with con.cursor() as cur:
                cur.executemany(
                    INSERT_SQL['StandardRoutes'], j['StandardRoutes']
                )


class Lines(WmataScraper, PostgresPublisher):
    """we only need to do this once, basically. This information is changed
    infrequently (per the wmata api docs, at least)

    """
    def __init__(self, api_key, url=URLS['Lines'], contentType='json',
                 dsnargs=None):
        WmataScraper.__init__(
            self, api_key=api_key, url=url, contentType=contentType
        )
        PostgresPublisher.__init__(self, dsnargs)

    def get(self):
        """simple wrapper to the wmata api standard routes end point

        args:
            None

        returns:
            tp (dict): requests library parsed json response from api

        raises:
            standard requests library errors

        """
        now = datetime.datetime.now()
        j = requests.get(
            url=self.url,
            params=self.params,
            headers=self.headers
        ).json()
        for row in j['Lines']:
            row['TimeStamp'] = now
        return j

    def publish(self, j):
        """publish a given api response to postgres"""
        with self.connect() as con:
            with con.cursor() as cur:
                cur.executemany(INSERT_SQL['Lines'], j['Lines'])


class StationInformation(WmataScraper, PostgresPublisher):
    """we only need to do this once, basically. This information is changed
    infrequently (per the wmata api docs, at least)

    """
    def __init__(self, api_key, url=URLS['StationInformation'],
                 contentType='json', dsnargs=None):
        WmataScraper.__init__(
            self, api_key=api_key, url=url, contentType=contentType
        )
        PostgresPublisher.__init__(self, dsnargs)

    def get(self):
        """simple wrapper to the wmata api station list end point

        args:
            None

        returns:
            tp (dict): requests library parsed json response from api

        raises:
            standard requests library errors

        """
        now = datetime.datetime.now()
        j = requests.get(
            url=self.url,
            params=self.params,
            headers=self.headers
        ).json()
        for row in j['Stations']:
            row.update(row.pop('Address'))
            row['TimeStamp'] = now
        return j

    def publish(self, j):
        """publish a given api response to postgres"""
        with self.connect() as con:
            with con.cursor() as cur:
                cur.executemany(
                    INSERT_SQL['StationInformation'], j['Stations']
                )


class StationToStationInformation(WmataScraper, PostgresPublisher):
    """we only need to do this once, basically. This information is changed
    infrequently (per the wmata api docs, at least)

    """
    def __init__(self, api_key, url=URLS['StationToStationInformation'],
                 contentType='json', dsnargs=None):
        WmataScraper.__init__(
            self, api_key=api_key, url=url, contentType=contentType
        )
        PostgresPublisher.__init__(self, dsnargs)

    def get(self):
        """simple wrapper to the wmata api station list end point

        args:
            None

        returns:
            tp (dict): requests library parsed json response from api

        raises:
            standard requests library errors

        """
        now = datetime.datetime.now()
        j = requests.get(
            url=self.url,
            params=self.params,
            headers=self.headers
        ).json()
        for row in j['StationToStationInfos']:
            row.update(row.pop('RailFare'))
            row['TimeStamp'] = now
        return j

    def publish(self, j):
        """publish a given api response to postgres"""
        with self.connect() as con:
            with con.cursor() as cur:
                cur.executemany(
                    INSERT_SQL['StationToStationInformation'],
                    j['StationToStationInfos']
                )


def run(fcredentials):
    with open(fcredentials, 'r') as f:
        dsnargs = yaml.load(f)
    api_key = dsnargs.pop('api_key')
    tp = TrainPositions(api_key=api_key, dsnargs=dsnargs)

    logger.info('starting to poll')
    while True:
        time.sleep(10)
        try:
            tp.publish(tp.get())
        except Exception as e:
            logger.exception(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-f', '--fcredentials', help='path to credentials file', required=True
    )
    args = parser.parse_args()
    run(fcredentials=args.fcredentials)
