select components,min(ts) as ts, max(endts) as endts, condition, direction from tempseg2.detector_segment_conditions where ts > '2012-01-01' and endts <= '2013-01-01' group by components,condition,direction order by components,ts limit 10;

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
