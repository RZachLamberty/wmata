#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module: map.py
Author: zlamberty
Created: 2017-02-05

Description:
    loading the wmata map and doing network sorts of shit to it

Usage:
    <usage>

"""

import argparse
import collections
import copy
import functools
import os

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import psycopg2

import eri.logging as logging


# ----------------------------- #
#   Module Constants            #
# ----------------------------- #

TRANSFER_TIME = pd.to_timedelta(1, unit='m')
SQL = {}
SQL['neighboring_stations'] = """SELECT
    linecode
    , circuitid
    , seqnum
    , LAG(stationcode) OVER (
        PARTITION BY
            linecode
            , tracknum
        ORDER BY
            seqnum
    ) AS prev_stationcode
    , stationcode
    , LEAD(stationcode) OVER (
        PARTITION BY
            linecode
            , tracknum
        ORDER BY
            seqnum
    ) AS next_stationcode
    , tracknum
FROM
    standard_routes
WHERE
    stationcode IS NOT NULL
"""
SQL['station_names'] = """SELECT
    code AS stationcode
    , name
    , lat
    , lon
FROM
    station_information
;
"""
SQL['transfers'] = """SELECT DISTINCT
    code AS code1
    , stationtogether1 AS code2
FROM
    station_information
WHERE
    stationtogether1 != ''
;
"""
SQL['arrivals_and_departures'] = """SELECT
    *
FROM
    arr_and_dep
WHERE
    directionnum = %(directionnum)s
    AND linecode = %(linecode)s
    AND (
        (stationcode = %(departure_station)s AND is_departure = 1)
        OR (stationcode = %(arrival_station)s AND is_arrival = 1)
    )
ORDER BY
    trainid
    , directionnum
    , timestamp
;"""
SQL['transit_time'] = """SELECT
    aod.*
FROM
    (
        SELECT
            aod.stationcode
            , aod.linecode
            , LEAD(aod.stationcode) OVER w AS next_stationcode
            , aod.directionnum
            , aod.trainid
            , aod.timestamp
            , LEAD(aod.timestamp) OVER w AS next_timestamp
            , (LEAD(aod.timestamp) OVER w) - aod.timestamp AS delta
            , is_departure
            , LEAD(aod.is_arrival) OVER w AS next_is_arrival
        FROM
            (
                SELECT
                    *
                FROM
                    arr_and_dep
                WHERE
                    directionnum = %(directionnum)s
                    AND (
                        (stationcode = %(departure_station)s AND is_departure = 1)
                        OR (stationcode = %(arrival_station)s AND is_arrival = 1)
                    )
            ) aod
        WINDOW w AS
            (
                PARTITION BY
                    directionnum
                    , linecode
                    , trainid
                ORDER BY
                    timestamp
            )
    ) aod
WHERE
    aod.is_departure = 1
    AND aod.next_is_arrival = 1
    AND delta < '1 day'
;"""


logger = logging.getLogger(__name__)
logging.configure()


# ----------------------------- #
#   connection dsn              #
# ----------------------------- #

DSN = collections.namedtuple('DSN', ['database', 'host', 'password', 'user'])


# ----------------------------- #
#   Main routine                #
# ----------------------------- #

def build_metro_network(dsn):
    """connect to a psql database and execute queries to build a network of
    station connections

    args:
        dsn (DSN named tuple): a DSN (named tuple defined above) of parameters
            that will be passed directly to a ``psycopg2`` connection object

    returns:
        n (``networkx.Graph``): an unidirected graph of edge connections, where
            the nodes have had names added as properties (but use stationcode as
            their primary identifier)

    raises:

    """
    with psycopg2.connect(**dsn._asdict()) as con:
        dfNeighbors = pd.read_sql(SQL['neighboring_stations'], con)
        dfNames = pd.read_sql(SQL['station_names'], con)
        dfTransfers = pd.read_sql(SQL['transfers'], con)

    #
    #  undirected
    #
    g = nx.from_pandas_dataframe(
        df=dfNeighbors[dfNeighbors.next_stationcode.notnull()],
        source='stationcode',
        target='next_stationcode'
    )

    gt = nx.from_pandas_dataframe(
        df=dfTransfers,
        source='code1',
        target='code2',
    )
    nx.set_edge_attributes(gt, 'station_transfer', True)

    g.add_edges_from(gt.edges(data=True))

    #
    # directed
    #

    # massage neighbor station dataframe such that we can add edge attributes of
    # line color and directionnum
    edges = pd.melt(
        frame=dfNeighbors,
        id_vars=['linecode', 'stationcode'],
        value_vars=['prev_stationcode', 'next_stationcode'],
        var_name='directionnum',
        value_name='dst_stationcode'
    ).drop_duplicates().replace(
        {
            'directionnum': {'prev_stationcode': 2, 'next_stationcode': 1}
        }
    )

    # create directed graph of connections between stations
    gDi = nx.from_pandas_dataframe(
        df=edges[edges.dst_stationcode.notnull()],
        source='stationcode',
        target='dst_stationcode',
        edge_attr=['linecode', 'directionnum'],
        create_using=nx.MultiDiGraph()
    )

    # create transfer edges
    gtDi = nx.from_pandas_dataframe(
        df=dfTransfers,
        source='code1',
        target='code2',
        create_using=nx.MultiDiGraph()
    )
    nx.set_edge_attributes(gtDi, 'station_transfer', True)

    gDi.add_edges_from(gtDi.edges(data=True))

    # add station names as attributes to the nodes (e.g. B03 --> Union Station)
    for (i, row) in dfNames.iterrows():
        g.node[row.stationcode]['name'] = row['name']
        gDi.node[row.stationcode]['name'] = row['name']
        g.node[row.stationcode]['latlon'] = row['lon'], row['lat']
        gDi.node[row.stationcode]['latlon'] = row['lon'], row['lat']

    return g, gDi


def get_station_codes(g, s):
    """take a string and return the station code(s) which is either that string
    (if it was already a station code) or the station code which has that string
    value for its station name. There can multiple names for a single code,
    hence the list instead of the single value

    """
    if g.has_node(s):
        return [s]
    else:
        return [n for (n, d) in g.nodes(data=True) if d.get('name', None) == s]


def get_paths(g, s0, s1):
    """given station codes or names, find all the non-ridiculous paths from s0
    to s1

    """
    return list({
        tuple(p)
        for s0now in get_station_codes(g, s0)
        for s1now in get_station_codes(g, s1)
        for p in nx.all_simple_paths(g, s0now, s1now)
    })


@functools.lru_cache(maxsize=None)
def path_to_straightshots(gDi, path):
    """a path is a list of station codes. some of those station codes are
    transfer points and some are pass-throughs. In a multidigraph, there can be
    multiple edges (train lines) connecting each station. Our goal in this
    function is to find the line codes, direction numbers, and start/endpoints
    of our straight shots and return only those.

    we really need a multidigraph here to do this -- an undirected graph won't
    be as helpful when it comes time to calculate timings, and a monograph won't
    encode line codes and required transfers (e.g. at Rosslyn, where we can fork
    without transferring)

    """
    if len(path) == 1:
        # base case for recursion, return nothing
        return [[]]

    sub = gDi.subgraph(path)

    a, b = path[:2]

    straightshots = []

    edges = sub[a][b].values()
    if any(e.get('station_transfer') for e in edges):
        furthest = b
        directionnum = None
        isTrans = True
    else:
        linecodes = {e['linecode'] for e in edges}
        # for whatever reason, train_positions never reports YLRP as the
        # linecode, only yello. skip YLRP for now.
        linecodes.discard('YLRP')

        directionnum = {e['directionnum'] for e in edges}
        if len(directionnum) > 1:
            raise ValueError(
                'more than one direction out of station {} for path {}'.format(
                    a, path
                )
            )
        directionnum = directionnum.pop()

        # get all stations we can reach using just the above lines in
        # direction directionnum. note: for this to really be a
        # straight shot we need to be able to access this station from
        # *every* line in linecodes, not just *any*. think of traveling from
        # georgia ave to pentagon via l'enfant -- you can take YL or GR to
        # l'enfant but only YL to pentagon; there are two straight shots, not
        # one. this is important for scenarios in which a transfer onto a mt.
        # vernon YL train is faster than waiting for a GR.
        reachable = collections.defaultdict(set)
        for (s0, s1, d) in sub.edges(data=True):
            if d.get('directionnum') == directionnum and d.get('linecode') != 'YLRP':
                reachable[s1].add(d.get('linecode'))
        # keep only those which are reachable by all of lineocdes
        reachable = {
            k
            for (k, v) in reachable.items()
            if all(lc in v for lc in linecodes)
        }
        furthest = max(reachable, key=lambda s: path.index(s))
        isTrans = False

    remainingPath = path[path.index(furthest):]

    straightshots += [
        [
            {
                'departure_station': a,
                'arrival_station': furthest,
                'directionnum': directionnum,
                'station_transfer': isTrans,
            }
        ] + restOfPath
        for restOfPath in path_to_straightshots(gDi, remainingPath)
    ]

    return straightshots


@functools.lru_cache(maxsize=None)
def get_travel_times(sDep, sArr, directionnum, dsn, d0=None, d1=None):
    """get all travel times from sDep to sArr, assuming those are straight shots
    (if they are not, we're sol and this query will do crazy things). In the
    future, d0 and d1 will serve as datetimes bounding the query; for now we're
    interested in every single trip.

    """
    params = {
        'departure_station': sDep,
        'arrival_station': sArr,
        'directionnum': directionnum
    }
    with psycopg2.connect(**dsn._asdict()) as con:
        return pd.read_sql(SQL['transit_time'], con, params=params)


def path_travel_times(gDi, path, dsn):
    straightshots = path_to_straightshots(gDi, path)
    ptts = []
    print('path = {}'.format(path))
    for straightshot in straightshots:
        # for the ith leg of our straightshot here, we look to get the following
        #     s{i} - the name of the {i}-th station
        #     s{i}arr - the arrival time at station s{i}
        #     s{i}dep - the departure time at station s{i}
        for (i, leg) in enumerate(straightshot):
            print('leg = {}'.format(leg))
            sArr = leg['arrival_station']
            sDep = leg['departure_station']
            directionnum = leg['directionnum']
            isTran = leg['station_transfer']

            if i == 0:
                if isTran:
                    raise NotImplementedError
                else:
                    dfTrip = get_travel_times(sDep, sArr, directionnum, dsn)
                    # rename "current" / "next" columns to be indexed
                    dfTrip = dfTrip.rename(
                        columns = {
                            'stationcode': 's0',
                            'next_stationcode': 's1',
                            'timestamp': 's0dep',
                            'next_timestamp': 's1arr',
                        }
                    )
                    dfTrip.loc[:, 's0arr'] = None
                    # drop column names we don't need
                    dfTrip = dfTrip[['s0', 's1', 's0arr', 's0dep', 's1arr']]
            else:
                if isTran:
                    dfTrip.loc[:, 's{}'.format(i + 1)] = sArr
                    dfTrip.loc[:, 's{}dep'.format(i)] = dfTrip['s{}arr'.format(i)]
                    dfTrip.loc[:, 's{}arr'.format(i + 1)] = dfTrip['s{}arr'.format(i)] + TRANSFER_TIME
                else:
                    dfLeg = get_travel_times(sDep, sArr, directionnum, dsn)

                    # rename "current" / "next" columns to be indexed
                    sNow = 's{}'.format(i)
                    sNowArr = 's{}arr'.format(i)
                    sNowDep = 's{}dep'.format(i)
                    sNext = 's{}'.format(i + 1)
                    sNextArr = 's{}arr'.format(i + 1)
                    sNextDep = 's{}dep'.format(i + 1)

                    dfLeg = dfLeg.rename(
                        columns = {
                            'stationcode': sNow,
                            'next_stationcode': sNext,
                            'timestamp': sNowDep,
                            'next_timestamp': sNextArr,
                        }
                    )
                    dfLeg = dfLeg[[sNow, sNext, sNowDep, sNextArr]]

                    # for each record in dfTrip, look up the first record in
                    # dfLeg for which we could make a transfer
                    dfNext = dfTrip.apply(
                        func=_find_next_connection, axis=1, args=(i, dfLeg)
                    )

                    dfTrip = dfTrip.join(dfNext)
        dfTrip = dfTrip[sorted(dfTrip.columns)]
        arr = dfTrip['s{}arr'.format(i + 1)].astype('<M8[ns]')
        dep = dfTrip['s0dep'].astype('<M8[ns]')
        dfTrip.loc[:, 'total_time'] = arr - dep
        ptts.append({
            'straightshot': straightshot,
            'travel_times': dfTrip.copy()
        })
    return ptts


def _find_next_connection(record, i, dfLeg):
    """given a single record from a dataset of multi-station trips (e.g. dfTrip
    above) and a full dataset of the legs from station i to station i + 1, find
    the first possible next leg of that trip

    """
    try:
        sNowArr = 's{}arr'.format(i)
        sNowDep = 's{}dep'.format(i)
        return dfLeg.iloc[
            dfLeg[dfLeg[sNowDep] >= record[sNowArr]][sNowDep].idxmin(),
            1:
        ]
    except (ValueError, TypeError):
        # was empty, return empty
        return pd.Series(None, index=dfLeg.columns[1:])


def plot_wmata(g, plottype='latlon'):
    if plottype == 'latlon':
        pos = nx.get_node_attributes(g, 'latlon')
    elif plottype == 'graphviz':
        pos = nx.drawing.nx_agraph.graphviz_layout(g)
    labels = nx.get_node_attributes(g, 'name')
    nx.draw(g, pos, labels=labels)
    plt.show()


def main(s0='Georgia Ave-Petworth', s1='Union Station'):
    dsn = DSN(
        database='wmata',
        user='wmata',
        password='wmata',
        host='ec2-54-152-41-113.compute-1.amazonaws.com'
    )
    g, gDi = build_metro_network(dsn)
    x = []
    for path in get_paths(g, s0, s1):
        for pttDict in path_travel_times(gDi, path, dsn):
            x.append({
                'straightshot': pttDict['straightshot'],
                'label': _straightshot_to_key(g, pttDict['straightshot']),
                'times': pttDict['travel_times'][['s0dep', 'total_time']],
            })

    f, ax = plt.subplots()
    for row in x:
        df = row['times'].total_time
        mint = pd.to_timedelta(0)
        maxt = pd.to_timedelta(3, unit='h')
        df = df[(df > mint) & (df <= maxt)]
        df = df.astype('timedelta64[s]')
        df.plot(kind='kde', ax=ax, label=row['label'])
    ax.legend()

    plt.show()

    return x, f, ax


def _straightshot_to_key(g, ss):
    k = g.node[ss[0]['departure_station']]['name']
    for leg in ss:
        scode = leg['arrival_station']
        k += ' --> {}'.format(g.node[scode]['name'])
    return k


# ----------------------------- #
#   Command line                #
# ----------------------------- #

def parse_args():
    """ Take a log file from the commmand line """
    parser = argparse.ArgumentParser()
    parser.add_argument("-x", "--xample", help="An Example", action='store_true')

    args = parser.parse_args()

    logger.debug("arguments set to {}".format(vars(args)))

    return args


if __name__ == '__main__':
    args = parse_args()
    main()
