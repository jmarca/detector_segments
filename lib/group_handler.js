var queue = require('queue-async')
var utils = require('./utils.js')

function group_handler(g,client,cb){
    if(g == null) cb(null)
    var q = queue(1) // do queries sequentially
    var renamed_detectors = g.detectors.map(function(did){
        return utils.convertDetectorIdForSQL(did)
    })
    var inclause =" IN('"+renamed_detectors.join("', '")+"')"
    var startts = g.ts;
    var stopts = g.nextts;

    console.log('processing '+g.detectors.length+' '+[startts,stopts].join(' to '))


    var insert_statement =["insert into tempseg.versioned_detector_segment_geometry"
                           ,"(detector_id,components,refnum,direction,seggeom)"
                           ,"WITH vrr AS ("
                           ,"select aa.*,"
                           ,"   nextval('tempseg.versioned_detector_ordering_sequence_id_seq') as versioned_sequence_id"
                           ,"  from ("
                           ,"   select distinct a.*"
                           ,"   from  tempseg.detector_route_relation a"
                           ,"   where detector_id"
                           , inclause
                           ,"    order by detector_sequence_id"
                           ,"  ) aa"
                           ,")"
                           ,"select qq.detector_id,qq.components,qq.refnum,qq.direction,qq.seggeom"
                           ,"from ("
                           ,"SELECT q.pdist,q.ptsec,q.ndist,q.detector_id,q.components,q.versioned_sequence_id,q.refnum,q.relation_direction as direction,"
                           ,"       CASE WHEN ptsec <= ntsec"
                           ,"                 THEN ST_line_substring( q.rls, q.ptsec,q.ntsec )"
                           ,"                 ELSE"
                           ,"                    case"
                           ,"                      WHEN mydist > q.pdist then ST_line_substring( q.rls, q.ptsec, 1 )"
                           ,"                      WHEN mydist < q.pdist then ST_line_substring( q.rls, 0, q.ntsec )"
                           ,"                    end"
                           ,"            END AS seggeom"
                           ,"       FROM"
                           ,"       ("
                           ,"          SELECT distinct vrrb.detector_id,array[p.detector_id,vrrb.detector_id,n.detector_id] as components,vrrb.versioned_sequence_id,vrrb.refnum,vrrb.relation_direction,"
                           ,"             vrrb.line AS rls,"
                           ,"             vrrb.dist AS mydist,"
                           ,"             vrrb.numline AS myline,"
                           ,"             p.dist AS pdist,"
                           ,"             CASE WHEN p.dist IS NOT NULL"
                           ,"                  THEN (vrrb.dist+p.dist)/2"
                           ,"                  ELSE 0"
                           ,"             END AS ptsec,"
                           ,"             n.dist AS ndist,"
                           ,"             CASE WHEN n.dist IS NOT NULL"
                           ,"                  THEN (vrrb.dist+n.dist)/2"
                           ,"                  ELSE 1"
                           ,"             END AS ntsec"
                           ,"          FROM vrr vrrb"
                           ,"          LEFT JOIN vrr p"
                           ,"               ON ( vrrb.versioned_sequence_id=p.versioned_sequence_id+1 AND vrrb.refnum=p.refnum AND vrrb.relation_direction=p.relation_direction AND vrrb.numline=p.numline)"
                           ,"          LEFT JOIN vrr n"
                           ,"               ON ( n.versioned_sequence_id=vrrb.versioned_sequence_id+1 AND vrrb.refnum=n.refnum AND vrrb.relation_direction=n.relation_direction AND vrrb.numline=n.numline)"
                           ,"          LEFT JOIN tempseg.numbered_route_lines rl on (rl.refnum=vrrb.refnum and vrrb.relation_direction=rl.direction)"
                           ,"          ORDER BY vrrb.versioned_sequence_id"
                           ,"       ) q"
                           ,"  ORDER BY refnum, direction, versioned_sequence_id"
                           ,") qq"
                           ,"LEFT OUTER JOIN tempseg.versioned_detector_segment_geometry existing ON"
                           ,"                (existing.direction = qq.direction and existing.components = qq.components)"
                           ,"WHERE existing.components is null"
                           ,"      and"
                           ,"      geometrytype(qq.seggeom) !~* 'point'"
                          ];

    var related_insert =["insert into tempseg2.detector_segment_conditions"
                         ,"(components,direction,ts,endts,condition)"
                         ,"WITH vrr AS ("
                         ,"select aa.*,"
                         ,"   nextval('tempseg.versioned_detector_ordering_sequence_id_seq') as versioned_sequence_id"
                         ,"  from ("
                         ,"   select distinct a.*"
                         ,"   from  tempseg.detector_route_relation a"
                         ,"   where detector_id"
                         ,inclause
                         ,"    order by detector_sequence_id"
                         ,"  ) aa"
                         ,")"
                         ,"select distinct qq.*"
                         ,"from("
                         ,"select q.components, q.direction"
                         ,", ('",startts,"'::text)::timestamp without time zone as ts"
                         ,", ('",stopts, "'::text)::timestamp without time zone  as endts"
                         ,",E'have imputed data'::text as condition"
                         ," from ("
                         ,"      SELECT array[p.detector_id,vrrb.detector_id,n.detector_id] as components, vrrb.relation_direction as direction"
                         ,"      FROM vrr vrrb"
                         ,"      LEFT JOIN vrr p"
                         ,"           ON ( vrrb.versioned_sequence_id=p.versioned_sequence_id+1 AND vrrb.refnum=p.refnum AND vrrb.relation_direction=p.relation_direction AND vrrb.numline=p.numline)"
                         ,"      LEFT JOIN vrr n"
                         ,"           ON ( n.versioned_sequence_id=vrrb.versioned_sequence_id+1 AND vrrb.refnum=n.refnum AND vrrb.relation_direction=n.relation_direction AND vrrb.numline=n.numline)"
                         ,"      left join tempseg.numbered_route_lines rl on (rl.refnum=vrrb.refnum and vrrb.relation_direction=rl.direction)"
                         ,"      ORDER BY vrrb.versioned_sequence_id"
                         ," ) q"
                         ,") qq"
                         ," join tempseg.versioned_detector_segment_geometry linked"
                         ," ON (linked.components=qq.components and linked.direction=qq.direction)"
                        ];

    console.log(insert_statement.join('\n'))
    console.log(related_insert.join('\n'))

    // while testing
    throw new Error('die')

    // query number one
    q.defer(function(cb2){
        var q1 = client.query(insert_statement.join(' '))
        q1.on('error'
              ,function(err){
                  if(err){
                      console.log("insert error" + JSON.stringify(err));
                      console.log(insert_statement.join('\n'));
                      return cb2(err);
                  }
                  return null
              });
        q1.on('end'
              ,function(result){
                  console.log('done with insert '+[startts,stopts].join(' to '))
                  // if(config.debug){
                  //     console.log(insert_statement.join('\n'))
                  // }
                  return cb2()
              });

        return null
    })

    // query number 2
    q.defer(function(cb2){
        var q2 = client.query(related_insert.join(' '))
        q2.on('error'
              ,function(err){
                  if(err){
                      console.log("insert related error" + JSON.stringify(err));
                      console.log(related_insert.join('\n'));
                      return cb2(err)
                  }
                  return null
              });
        q2.on('end'
              ,function(result){
                  console.log('done with related insert '+[startts,stopts].join(' to '))
                  // if(config.debug){
                  //     console.log(related_insert.join('\n'))
                  // }
                  return cb2()
              });

    })
    q.await(function(e,r){
        if(e){
            return cb(e) // echo die to outer callback
        }
        return cb()
    })
    return null;
}

module.exports=group_handler
