/*
    getting stations and neighboring stations
*/
SELECT
    linecode
    , circuitid
    , seqnum
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
;


/*  getting station codes and names */
SELECT
    code AS stationcode
    , name
FROM
    station_information
;


/*  disembarking from stations */
select
    d.stationcode
    , d.directionnum
    , d.linecode
    , d.trainid
    , d.timestamp
    , case
        when d.time_till_next_record is null then
            1
        when d.time_till_next_record > '00:10:00' then
            1
        else
            0
    end as is_departure
    , case
        when d.time_since_last_record is null then
            1
        when d.time_since_last_record > '00:10:00' then
            1
        else
            0
    end as is_arrival
into
    arr_and_dep
from
    (
        select
            sr.stationcode
            , tp.circuitid
            , tp.directionnum
            , tp.linecode
            , tp.trainid
            , tp.secondsatlocation
            , tp.timestamp
            /*  we originally tried to use any record where the secondsatlocation
                for this record was larger than the secondsatlocation for the
                next, but this was problematic. I can't say why, exactly, but it
                appears that the process responsible for populating those
                values can be reset. Trains which clearly sat at stations for
                many minutes would report "leaving" and "arriving" several
                times in a row.
                E.g.: stationcode C12, circuitid 1170, directionnum 2,
                      trainid 260
            */
            , lead(tp.timestamp) over w - tp.timestamp as time_till_next_record
            , tp.timestamp - lag(tp.timestamp) over w as time_since_last_record
        from
            train_positions tp
            join
            (
                select distinct
                    circuitid
                    , stationcode
                from
                    standard_routes
                where
                    stationcode is not null
            ) sr
            on
                tp.circuitid = sr.circuitid
        window w as
            (
                partition by
                    sr.stationcode
                    , tp.linecode
                    , tp.trainid
                    , tp.directionnum
                order by
                    tp.timestamp
            )
    ) d
where
    /*  arbitrary threshold of 10 minutes */
    (
        d.time_till_next_record is null
        or  d.time_till_next_record > '00:10:00'
    )
    or (
        d.time_since_last_record is null
        or d.time_since_last_record > '00:10:00'
     )
;


/* making sure we don't have crazy numbers for trains in stations per day */
select
    x.*
    , count(*) as ct
from
    (
        select
            stationcode
            , directionnum
            , trainid
            , date_trunc('day', timestamp) as d
        from
            arr_and_dep
    ) x
group by
    stationcode
    , directionnum
    , trainid
    , d
order by
    ct desc
;

/*  check out a particularly odd case selected based on the above (2017-02-24) */
select
    *
from
    arr_and_dep
where
    '2017-02-19' < timestamp
    and '2017-02-20' <= timestamp
    and directionnum = 2
    and trainid = '191'
    and stationcode = 'J03'
;


/*  given a start and end code, find the list of travel times from one station
    to the next
*/
select
    *
from
    arr_and_dep
where
    directionnum = 2
    and linecode = 'GR'
    and (
        (stationcode = 'E05' and is_departure = 1)
        or (stationcode = 'F03' and is_arrival = 1)
    )
order by
    trainid
    , directionnum
    , timestamp
;

/*  find travel times from the above query */
select
    aod.*
from
    (
        select
            aod.stationcode
            , lead(aod.stationcode) over w as next_stationcode
            , aod.directionnum
            , aod.trainid
            , aod.timestamp
            , lead(aod.timestamp) over w as next_timestamp
            , (lead(aod.timestamp) over w) - aod.timestamp as diff
            , is_departure
            , lead(aod.is_arrival) over w as next_is_arrival
        from
            (
                select
                    *
                from
                    arr_and_dep
                where
                    directionnum = 2
                    and linecode = 'GR'
                    and (
                        (stationcode = 'E05' and is_departure = 1)
                        or (stationcode = 'F01' and is_arrival = 1)
                    )
            ) aod
        window w as
            (
                partition by
                    directionnum
                    , linecode
                    , trainid
                order by
                    timestamp
            )
    ) aod
where
    aod.is_departure = 1
    and aod.next_is_arrival = 1
    and diff < '1 day'
;


/*  very similar sort of thing here, but with a specified departure station,
    arrival station, and direction
*/
select
    d.stationcode
    , d.directionnum
    , d.trainid
    , d.timestamp
    , case
        when d.time_till_next_record is null then
            1
        when d.time_till_next_record > '00:10:00' then
            1
        else
            0
    end as is_departure
    , case
        when d.time_since_last_record is null then
            1
        when d.time_since_last_record > '00:10:00' then
            1
        else
            0
    end as is_arrival
into
    temp derp
from
    (
        select
            sr.stationcode
            , tp.circuitid
            , tp.directionnum
            , tp.trainid
            , tp.secondsatlocation
            , tp.timestamp
            /*  we originally tried to use any record where the secondsatlocation
                for this record was larger than the secondsatlocation for the
                next, but this was problematic. I can't say why, exactly, but it
                appears that the process responsible for populating those
                values can be reset. Trains which clearly sat at stations for
                many minutes would report "leaving" and "arriving" several
                times in a row.
                E.g.: stationcode C12, circuitid 1170, directionnum 2,
                      trainid 260
            */
            , (lead(tp.timestamp) over w) - tp.timestamp as time_till_next_record
            , tp.timestamp - (lag(tp.timestamp) over w) as time_since_last_record
        from
            (
                select
                    *
                from
                    train_positions
                where
                    directionnum = 1
                    and linecode = 'GR'
            ) tp
            join
            (
                select distinct
                    circuitid
                    , stationcode
                from
                    standard_routes
                where
                    stationcode = 'E05'
                    or stationcode = 'E06'
            ) sr
            on
                tp.circuitid = sr.circuitid
        window w as
            (
                partition by
                    trainid
                order by
                    timestamp
            )
    ) d
where
    /*  arbitrary threshold of 10 minutes */
    (
        d.time_till_next_record is null
        or  d.time_till_next_record > '00:10:00'
    )
    or (
        d.time_since_last_record is null
        or d.time_since_last_record > '00:10:00'
     )
;



/*  off the arr_and_dep table, find travel times */
