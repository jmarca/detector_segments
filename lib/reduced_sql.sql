--
--  this sql will reduce the multiple entries in detector_segment_conditions
--  into single entries in reduced_detector_segment_conditions
--
--  needs to be tweaked though.  At the moment it presumes that all
--  rows are continuous...that is, if component A has an entry at time
--  t1 and max entry at tn, the presumption is that component A also
--  has an entry for every time step from 1 to n.  In fact there is no
--  guarantee that this is the case.  In the worst case, it could be
--  that a component is only active in the first hour and the last
--  hour of a year, but the below query will screw up and make it
--  active for every hour.
--


insert into tempseg.reduced_detector_segment_conditions
(components,ts, endts, condition, direction)
with reduction as (
    SELECT components,min(ts) as ts, max(endts) as endts, condition, direction
    FROM tempseg2.detector_segment_conditions
    WHERE ts > '2012-01-01' AND endts <= '2013-01-01'
    GROUP BY components,condition,direction
    ORDER BY components,ts
    )
select reduction.*
from reduction
left outer join
tempseg.reduced_detector_segment_conditions existing using (ts, condition, components, direction)
where existing.ts is null;


-- hmm.

with reduction as (
    SELECT components,   sum( EXTRACT(DAY FROM (endts -  ts)) *24
      + EXTRACT(HOUR FROM (endts -  ts))) as countedhours,
    EXTRACT(DAY FROM (max(endts) -  min(ts))) *24
      + EXTRACT(HOUR FROM (max(endts) -  min(ts))) as hoursdiff,
    min(ts) as ts,
    max(endts) as endts, condition, direction
    FROM tempseg2.detector_segment_conditions
    WHERE ts > '2012-01-01' AND endts <= '2013-01-01'
    GROUP BY components,condition,direction
    ORDER BY components,ts
    )
select * from reduction where
countedhours != hoursdiff
 order by hoursdiff - countedhours;

-- okay, here is a result.

--                  components                  | countedhours | hoursdiff |         ts          |        endts        |     condition     | direction
-- ---------------------------------------------+--------------+-----------+---------------------+---------------------+-------------------+-----------
--  {wimid_26,wimid_22,wimid_23}                |        15494 |      7799 | 2012-02-11 00:00:00 | 2012-12-31 23:00:00 | have imputed data | east
--  {wimid_23,wimid_22,wimid_26}                |        15494 |      7799 | 2012-02-11 00:00:00 | 2012-12-31 23:00:00 | have imputed data | west
--  {wimid_22,wimid_23,NULL}                    |        15494 |      7799 | 2012-02-11 00:00:00 | 2012-12-31 23:00:00 | have imputed data | east
--  {NULL,wimid_23,wimid_22}                    |        15494 |      7799 | 2012-02-11 00:00:00 | 2012-12-31 23:00:00 | have imputed data | west
--  {wimid_2,wimid_30,NULL}                     |        14138 |      8782 | 2012-01-01 01:00:00 | 2012-12-31 23:00:00 | have imputed data | north
--  {wimid_108,wimid_2,wimid_30}                |        14138 |      8782 | 2012-01-01 01:00:00 | 2012-12-31 23:00:00 | have imputed data | north
--  {wimid_30,wimid_2,wimid_108}                |        14138 |      8782 | 2012-01-01 01:00:00 | 2012-12-31 23:00:00 | have imputed data | south
--  {NULL,wimid_30,wimid_2}                     |        14138 |      8782 | 2012-01-01 01:00:00 | 2012-12-31 23:00:00 | have imputed data | south
--  {NULL,wimid_20,NULL}                        |        11462 |      8725 | 2012-01-03 11:00:00 | 2013-01-01 00:00:00 | have imputed data | north
--  {wimid_81,wimid_76,wimid_94}                |        11462 |      8725 | 2012-01-03 11:00:00 | 2013-01-01 00:00:00 | have imputed data | north
--  {wimid_94,wimid_76,wimid_81}                |        11462 |      8725 | 2012-01-03 11:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {NULL,wimid_99,NULL}                        |         4849 |      3363 | 2012-08-13 21:00:00 | 2013-01-01 00:00:00 | have imputed data | west
--  {NULL,wimid_99,NULL}                        |         4849 |      3363 | 2012-08-13 21:00:00 | 2013-01-01 00:00:00 | have imputed data | east
--  {NULL,vdsid_1214853,vdsid_1215236}          |         5954 |      6720 | 2012-03-27 00:00:00 | 2013-01-01 00:00:00 | have imputed data | east
--  {vdsid_1016710,vdsid_1023310,vdsid_1023210} |         6941 |      8783 | 2012-01-01 01:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_1023510,vdsid_1016710,vdsid_1023310} |         6941 |      8783 | 2012-01-01 01:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_317466,vdsid_317389,vdsid_317229}    |         6872 |      8783 | 2012-01-01 01:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_317389,vdsid_317229,vdsid_317381}    |         6872 |      8783 | 2012-01-01 01:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_1201941,vdsid_1209336,vdsid_1209319} |         6041 |      8249 | 2012-01-05 10:00:00 | 2012-12-14 03:00:00 | have imputed data | south
--  {vdsid_1022410,vdsid_1073410,vdsid_1022710} |         1620 |      3961 | 2012-07-19 16:00:00 | 2012-12-31 17:00:00 | have imputed data | north
--  {wimid_77,vdsid_810751,vdsid_810764}        |         1482 |      3936 | 2012-07-21 00:00:00 | 2013-01-01 00:00:00 | have imputed data | east
--  {vdsid_1119251,vdsid_1119232,NULL}          |          954 |      3680 | 2012-07-31 16:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_715996,vdsid_759772,vdsid_716965}    |         2546 |      5850 | 2012-01-18 11:00:00 | 2012-09-18 05:00:00 | have imputed data | south
--  {vdsid_402658,vdsid_402654,vdsid_402652}    |         5219 |      8725 | 2012-01-03 11:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_402664,vdsid_402658,vdsid_402654}    |         5219 |      8725 | 2012-01-03 11:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {NULL,vdsid_806025,vdsid_806019}            |         4302 |      8013 | 2012-01-01 07:00:00 | 2012-11-30 04:00:00 | have imputed data | south
--  {vdsid_1013910,vdsid_1006410,vdsid_1068610} |         3775 |      8031 | 2012-02-01 09:00:00 | 2013-01-01 00:00:00 | have imputed data | north
--  {vdsid_1006410,vdsid_1068610,vdsid_1006610} |         3775 |      8031 | 2012-02-01 09:00:00 | 2013-01-01 00:00:00 | have imputed data | north
--  {vdsid_801535,vdsid_801527,vdsid_801517}    |         1198 |      5955 | 2012-01-06 11:00:00 | 2012-09-10 14:00:00 | have imputed data | west
--  {vdsid_1114708,vdsid_1118326,NULL}          |         2571 |      7800 | 2012-02-11 00:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_1111547,vdsid_1117979,vdsid_1121387} |         2326 |      7800 | 2012-02-11 00:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_1117979,vdsid_1121387,vdsid_1121394} |         2326 |      7800 | 2012-02-11 00:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_819183,vdsid_819185,vdsid_819060}    |         3099 |      8702 | 2012-01-04 10:00:00 | 2013-01-01 00:00:00 | have imputed data | east
--  {vdsid_819185,vdsid_819060,NULL}            |          637 |      6240 | 2012-01-04 10:00:00 | 2012-09-20 10:00:00 | have imputed data | east
--  {vdsid_819061,vdsid_819184,vdsid_819182}    |         3037 |      8640 | 2012-01-07 00:00:00 | 2013-01-01 00:00:00 | have imputed data | west
--  {NULL,vdsid_819061,vdsid_819184}            |          575 |      6178 | 2012-01-07 00:00:00 | 2012-09-20 10:00:00 | have imputed data | west
--  {vdsid_317213,vdsid_315006,vdsid_317342}    |         1904 |      8783 | 2012-01-01 01:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_317335,vdsid_317206,vdsid_315969}    |         1901 |      8780 | 2012-01-01 03:00:00 | 2012-12-31 23:00:00 | have imputed data | north
--  {vdsid_315938,vdsid_317213,vdsid_315006}    |         1339 |      8218 | 2012-01-24 14:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_315054,vdsid_317335,vdsid_317206}    |         1819 |      8698 | 2012-01-04 13:00:00 | 2012-12-31 23:00:00 | have imputed data | north
--  {vdsid_811314,vdsid_801418,vdsid_1208230}   |         1117 |      8621 | 2012-01-06 11:00:00 | 2012-12-30 16:00:00 | have imputed data | west
--  {vdsid_1208226,vdsid_801415,vdsid_801422}   |         1240 |      8744 | 2012-01-01 08:00:00 | 2012-12-30 16:00:00 | have imputed data | east
--  {vdsid_801418,vdsid_1208230,vdsid_1208208}  |         1117 |      8621 | 2012-01-06 11:00:00 | 2012-12-30 16:00:00 | have imputed data | west
--  {vdsid_1119666,vdsid_1108473,vdsid_1108471} |         1071 |      8776 | 2012-01-01 01:00:00 | 2012-12-31 17:00:00 | have imputed data | south
--  {vdsid_1108475,vdsid_1119666,vdsid_1108473} |         1071 |      8776 | 2012-01-01 01:00:00 | 2012-12-31 17:00:00 | have imputed data | south
--  {vdsid_317365,vdsid_317354,vdsid_315955}    |          608 |      8783 | 2012-01-01 01:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_317199,vdsid_317365,vdsid_317354}    |          608 |      8783 | 2012-01-01 01:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_766642,vdsid_765456,vdsid_717361}    |          383 |      8728 | 2012-01-03 08:00:00 | 2013-01-01 00:00:00 | have imputed data | west
--  {vdsid_766587,vdsid_766642,vdsid_765456}    |          383 |      8728 | 2012-01-03 08:00:00 | 2013-01-01 00:00:00 | have imputed data | west
--  {vdsid_1204565,vdsid_1204538,vdsid_1204521} |          112 |      8783 | 2012-01-01 01:00:00 | 2013-01-01 00:00:00 | have imputed data | south
--  {vdsid_1204538,vdsid_1204521,vdsid_1204507} |          112 |      8783 | 2012-01-01 01:00:00 | 2013-01-01 00:00:00 | have imputed data | south
-- (51 rows)


select * from tempseg2.detector_segment_conditions where
components =  '{"vdsid_1000210","vdsid_1000310","vdsid_1000510"}' ;

select * from tempseg2.detector_segment_conditions
where
components = '{"wimid_26","wimid_22","wimid_23"}'
and  ts > '2012-01-01' AND endts <= '2013-01-01'
order by ts;
