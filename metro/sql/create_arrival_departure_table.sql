/*  handle pre-existing table */
begin;
drop table if exists arr_and_dep;
commit;


/*  record arrival and departure times for all trains throughout history */
begin;
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
commit;
