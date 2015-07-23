--insert into tempseg.versioned_detector_segment_geometry
--(detector_id,components,refnum,direction,seggeom)
WITH vrr AS (
select aa.*,
   nextval('tempseg.versioned_detector_ordering_sequence_id_seq') as versioned_sequence_id
  from (
   select distinct a.*
   from  tempseg.detector_route_relation a
   where detector_id
 IN('vdsid_500010093', 'vdsid_500010101', 'vdsid_500010122', 'vdsid_500010133', 'vdsid_500010142', 'vdsid_500010152', 'vdsid_500011021', 'vdsid_500011032', 'vdsid_500011042', 'vdsid_500011052', 'vdsid_500011063', 'vdsid_500011072', 'vdsid_500011092', 'vdsid_500011112', 'vdsid_500011121', 'vdsid_500011143')
    order by detector_sequence_id
  ) aa
)
select qq.detector_id,qq.components,qq.refnum,qq.direction, st_length(st_transform(qq.seggeom,32611)) * 0.000621371192 as len  --qq.seggeom
from (
SELECT q.pdist,q.ptsec,q.ndist,q.detector_id,q.components,q.versioned_sequence_id,q.refnum,q.relation_direction as direction,
       CASE WHEN ptsec <= ntsec
                 THEN ST_line_substring( q.rls, q.ptsec,q.ntsec )
                 ELSE
                    case
                      WHEN mydist > q.pdist then ST_line_substring( q.rls, q.ptsec, 1 )
                      WHEN mydist < q.pdist then ST_line_substring( q.rls, 0, q.ntsec )
                    end
            END AS seggeom
       FROM
       (
          SELECT distinct vrrb.detector_id,array[p.detector_id,vrrb.detector_id,n.detector_id] as components,vrrb.versioned_sequence_id,vrrb.refnum,vrrb.relation_direction,
             vrrb.line AS rls,
             vrrb.dist AS mydist,
             vrrb.numline AS myline,
             p.dist AS pdist,
             CASE WHEN p.dist IS NOT NULL
                  THEN (vrrb.dist+p.dist)/2
                  ELSE 0
             END AS ptsec,
             n.dist AS ndist,
             CASE WHEN n.dist IS NOT NULL
                  THEN (vrrb.dist+n.dist)/2
                  ELSE 1
             END AS ntsec
          FROM vrr vrrb
          LEFT JOIN vrr p
               ON ( vrrb.versioned_sequence_id=p.versioned_sequence_id+1 AND vrrb.refnum=p.refnum AND vrrb.relation_direction=p.relation_direction AND vrrb.numline=p.numline)
          LEFT JOIN vrr n
               ON ( n.versioned_sequence_id=vrrb.versioned_sequence_id+1 AND vrrb.refnum=n.refnum AND vrrb.relation_direction=n.relation_direction AND vrrb.numline=n.numline)
          LEFT JOIN tempseg.numbered_route_lines rl on (rl.refnum=vrrb.refnum and vrrb.relation_direction=rl.direction)
          ORDER BY vrrb.versioned_sequence_id
       ) q
  ORDER BY refnum, direction, versioned_sequence_id
) qq
LEFT OUTER JOIN tempseg.versioned_detector_segment_geometry existing ON
                (existing.direction = qq.direction and existing.components = qq.components)
WHERE existing.components is null
      and
      geometrytype(qq.seggeom) !~* 'point'


--insert into tempseg2.detector_segment_conditions
--(components,direction,ts,endts,condition)
WITH vrr AS (
select aa.*,
   nextval('tempseg.versioned_detector_ordering_sequence_id_seq') as versioned_sequence_id
  from (
   select distinct a.*
   from  tempseg.detector_route_relation a
   where detector_id
 IN('vdsid_500010093', 'vdsid_500010101', 'vdsid_500010122', 'vdsid_500010133', 'vdsid_500010142', 'vdsid_500010152', 'vdsid_500011021', 'vdsid_500011032', 'vdsid_500011042', 'vdsid_500011052', 'vdsid_500011063', 'vdsid_500011072', 'vdsid_500011092', 'vdsid_500011112', 'vdsid_500011121', 'vdsid_500011143')
    order by detector_sequence_id
  ) aa
)
select distinct qq.*
from(
select q.components, q.direction
, ('
2012-03-06 16:00:00
'::text)::timestamp without time zone as ts
, ('
2013-01-01 00:00:00
'::text)::timestamp without time zone  as endts
,E'have imputed data'::text as condition
 from (
      SELECT array[p.detector_id,vrrb.detector_id,n.detector_id] as components, vrrb.relation_direction as direction
      FROM vrr vrrb
      LEFT JOIN vrr p
           ON ( vrrb.versioned_sequence_id=p.versioned_sequence_id+1 AND vrrb.refnum=p.refnum AND vrrb.relation_direction=p.relation_direction AND vrrb.numline=p.numline)
      LEFT JOIN vrr n
           ON ( n.versioned_sequence_id=vrrb.versioned_sequence_id+1 AND vrrb.refnum=n.refnum AND vrrb.relation_direction=n.relation_direction AND vrrb.numline=n.numline)
      left join tempseg.numbered_route_lines rl on (rl.refnum=vrrb.refnum and vrrb.relation_direction=rl.direction)
      ORDER BY vrrb.versioned_sequence_id
 ) q
) qq
 join tempseg.versioned_detector_segment_geometry linked
 ON (linked.components=qq.components and linked.direction=qq.direction)


insert into tempseg2.detector_segment_conditions
(components,direction,ts,endts,condition)

WITH vrr AS (
select aa.*,
   nextval('tempseg.versioned_detector_ordering_sequence_id_seq') as versioned_sequence_id
  from (
   select distinct a.*
   from  tempseg.detector_route_relation a
   where detector_id
 IN('vdsid_500010093', 'vdsid_500010101', 'vdsid_500010122', 'vdsid_500010133', 'vdsid_500010142', 'vdsid_500010152', 'vdsid_500011021', 'vdsid_500011032', 'vdsid_500011042', 'vdsid_500011052', 'vdsid_500011063', 'vdsid_500011072', 'vdsid_500011092', 'vdsid_500011112', 'vdsid_500011121', 'vdsid_500011143')
    order by detector_sequence_id
  ) aa
)
select distinct qq.*
from(
select q.components, q.direction
, ('
2012-03-06 16:00:00
'::text)::timestamp without time zone as ts
, ('
2013-01-01 00:00:00
'::text)::timestamp without time zone  as endts
,E'have imputed data'::text as condition
 from (
      SELECT array[p.detector_id,vrrb.detector_id,n.detector_id] as components, vrrb.relation_direction as direction
      FROM vrr vrrb
      LEFT JOIN vrr p
           ON ( vrrb.versioned_sequence_id=p.versioned_sequence_id+1 AND vrrb.refnum=p.refnum AND vrrb.relation_direction=p.relation_direction AND vrrb.numline=p.numline)
      LEFT JOIN vrr n
           ON ( n.versioned_sequence_id=vrrb.versioned_sequence_id+1 AND vrrb.refnum=n.refnum AND vrrb.relation_direction=n.relation_direction AND vrrb.numline=n.numline)
      left join tempseg.numbered_route_lines rl on (rl.refnum=vrrb.refnum and vrrb.relation_direction=rl.direction)
      ORDER BY vrrb.versioned_sequence_id
 ) q
) qq
 join tempseg.versioned_detector_segment_geometry linked
 ON (linked.components=qq.components and linked.direction=qq.direction)
left outer join tempseg2.detector_segment_conditions e
   ON (e.ts=qq.ts
       and e.condition=qq.condition
       and e.components=qq.components
       and e.direction=qq.direction)
 where e.ts is null;
