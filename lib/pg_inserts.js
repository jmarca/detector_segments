// psql stuff
var pg = require('pg');

var _=require('lodash')

var env = process.env;

// make sure that this works with config_okay, revent approach to
// config.json
var config
function pg_handler(_config){
    config = _config
    if(options === undefined){ options = {} }
    var host = options.host     ? options.host     :  env.PSQL_HOST;
    var user = options.username ? options.username :  env.PSQL_USER;
    var port = options.port     ? options.port     :  5432;
    var osmConnectionString        = "pg://"+user+"@"+host+":"+port+"/osm";


    function hit_pg(data,next){
        pg.connect(osmConnectionString,
                   do_inserts(data,function(err){
                       if(err) {
                           console.log(err)
                           throw new Error()
                       }
                       console.log('all done')
                       pg.end()
                       next()
                       return null
                   })
                  );
    }
    return hit_pg
}

exports.pg_handler=pg_handler

// fixme to properly use client, close connection, all that

function do_inserts(detector_groups,next){
    return function(err,client){
        if(err){
            console.log('connection error '+JSON.stringify(err));
            return next(err);
        }
        // have a valid connection, so rattle off the inserts sequentially
        _.each(detector_groups
              ,function(g){
                  if(g == null) return null;
                   var inclause =" IN('"+g.active.join("', '")+"')"
                   var startts = g.ts;
                   var stopts = g.nextts;

                  console.log('processing '+g.active.length+' '+[startts,stopts].join(' to '))


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

                  var q1 = client.query(insert_statement.join(' '))
                  q1.on('error'
                       ,function(err){
                            if(err){
                                console.log("insert error" + JSON.stringify(err));
                                console.log(insert_statement.join('\n'));
                                return next('die');
                            }
                            return null
                        });
                  q1.on('end'
                       ,function(result){
                           console.log('done with insert '+[startts,stopts].join(' to '))
                           if(config.debug){
                               console.log(insert_statement.join('\n'))
                           }
                            return null
                        });
                   var q2 = client.query(related_insert.join(' '))
                   q2.on('error'
                        ,function(err){
                             if(err){
                                 console.log("insert related error" + JSON.stringify(err));
                                 console.log(related_insert.join('\n'));
                                 return next('die');
                             }
                             return null
                         });
                   q2.on('end'
                        ,function(result){
                             console.log('done with related insert '+[startts,stopts].join(' to '))
                           if(config.debug){
                               console.log(related_insert.join('\n'))
                           }
                             return null;
                         });
                   return null;
               });
        client.on('drain', function() {
            console.log("drained all queries");
            // I recall something other than this is used these days
            client.end()
            return next(null)
        });
        console.log('done queueing queries')
        return null;
    };
}
