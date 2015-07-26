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
