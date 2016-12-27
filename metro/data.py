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
URL_TRAIN_POSITIONS = '{}TrainPositions/TrainPositions'.format(URL_BASE)

INSERT_TRAIN_POS = """INSERT INTO
    {tblname:}
VALUES (
    %(CarCount)s, %(CircuitId)s, %(DestinationStationCode)s, %(DirectionNum)s,
    %(LineCode)s, %(SecondsAtLocation)s, %(ServiceType)s, %(TrainId)s,
    %(TimeStamp)s
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
    def get(self, *args, **kwargs):
        raise NotImplementedError

    def publish(self, *args, **kwargs):
        raise NotImplementedError


class PostgresPublisher(object):
    """mixin for publishing to local postgres database"""
    def __init__(self, **dsnargs):
        """all arguments are passed through *directly* to a psycopg2 connect
        function, so see that function for full documentation

        """
        self.dsnargs = dsnargs

    def connect(self):
        return psycopg2.connect(**self.dsnargs)


class TrainPositions(WmataScraper, PostgresPublisher):
    """the live of position trains is exposed by a single url, with data updated
    approximately every 10 seconds. This object is dumb -- simple query, simple
    persistence (postgres assumed)

    """
    def __init__(self, api_key, url=URL_TRAIN_POSITIONS, dsnargs=None):
        self.params = {'contentType': 'json'}
        self.headers = {'api_key': api_key}
        self.url = url

        # make db connection, passthrough is super awkard. Maybe I should go
        # back to yaml credentials?
        if dsnargs is None:
            raise WmataParseError(
                "you must supply either a connection string (pgargs) or "
                "connection parameters (pgkwargs)"
            )
        PostgresPublisher.__init__(self, **dsnargs)

    def get(self):
        """simple wrapper to the wmata api train positions end point

        args:
            api_key (str): wmata api key
            url (str): url for the wmata api endpoint (default:
                metro.data.URL_TRAIN_POSTITIONS)

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

    def publish(self, j, tblname='train_positions'):
        """publish a given api response to postgres"""
        with self.connect() as con:
            with con.cursor() as cur:
                cur.executemany(
                    INSERT_TRAIN_POS.format(tblname=tblname),
                    j['TrainPositions']
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
