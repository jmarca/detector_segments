/* global require console process */

/** fix the mess of maps and segment lengths
 *
 * Sadly, this is necesary because my prior brillian plan has no legs
 *
 * the general idea is to take the detectors for each year along with
 * earliest and latest imputation times from teh imputation database.
 *
 * the detectors shoud be pulled from all databases and stored ina big old hash
 * with earliest and laste times of imputation done up in red lights.
 *
 * Ideally, if a detector is on it is always on, so there should be a
 * big blob in the middle of the eyar where stuff is realively stable
 *
 * detector00 and detectorwim dbs need to be queries, along iwth all
 * the regular numbers
 *
 * then from each of the detectors (htousands at most), pull of the
 * start and end times into a single list using union or whatnot to
 * get sorted, non-repeating list of event times.  the last entry in
 * the list is the last time of the year, I hope
 *
 * For each of the event times excepting the last eent time, run
 * thorugh a loop that stores the active detectors for that time until
 * the next time, and then turns the crank on that funciton in the osm
 * database to generate the list of segments, and then saves such
 * segments.
 *
 * pull from the perl script to do a lot of the sql stuff
 *
 */

var superagent = require('superagent')
var _ = require('lodash');
var querystring = require('querystring');
var async = require('async');

var geom_utils=require('geom_utils')
var postgis_utils=require('shapes_postgis').postgis_utils

var options = {};

var env = process.env;
var cuser = options.user || env.COUCHDB_USER ;
var cpass = options.pass || env.COUCHDB_PASS ;
var chost = options.host || env.COUCHDB_HOST ;
var cport = options.port || env.COUCHDB_PORT || 5984;
var trackerdbname = options.trackerdbname || env.COUCHDB_TRACKINGDB || 'vdsdata%2ftracking';

var couch = 'http://'+chost+':'+cport;
var tracker = couch +'/'+trackerdbname;


// psql stuff
var pg = require('pg');
var host = options.host     ? options.host     :  env.PSQL_HOST;
var user = options.username ? options.username :  env.PSQL_USER;
var pass = options.password ? options.password :  env.PSQL_PASS;
var port = options.port ? options.port :  5432;
var osmConnectionString        = "pg://"+user+":"+pass+"@"+host+":"+port+"/osm";


// store stuff here

var detectors={'2007':{}
              ,'2009':{}
              ,'2008':{}
              };
var years={};

// steps:
// 1. get all detectors, start and end times.  requires processing all
//    databases. processing can be async, but must complete before task 2
// 2. generate event times
// 3. for each event time, generate components and maps code, and save it.
//    I could possibly split that up into freeways at a time
//    I could possibly run this in parallel, depending on load on DB
//


function min_max_detectors(db,next){
    // poke the couchdb, get a list of all detectors, get min and max times

    var dcouch = 'http://'+db.host+':'+cport;
    var source = dcouch + db.db;
    var view = db.view || '_design/collect2/_view/idymdh_array';
    var query = {group:true
                ,reduce:true
                ,group_level:2
                };
    if (view = '_all_docs'){
        query = {}
    }

    var url = source+'/'+view + '?'+geom_utils.toQuery(query)
    console.log(url)
    throw new Error('die now')

    // var min_max_detector = function(source,view){
    //                            var q = {reduce:false
    //                                    ,descending:false
    //                                    ,limit:1}
    //                            var p = {reduce:false
    //                                    ,descending:true
    //                                    ,limit:1}
    //                            var wim_800_regex = /wim.8\d\d/;
    //                            return function(detector,cb){
    //                                if(detector === undefined
    //                                   || detector.key === undefined
    //                                   || detector.key[0] == null
    //                                   || wim_800_regex.test(detector.key[0]+'')
    //                                   || detector.key[1] === undefined  ) return cb(null)
    //                                var startkey = _.extend({'startkey':detector.key},q)
    //                                var url_first =  source+'/'+view + '?'+geom_utils.toQuery(startkey);
    //                                startkey = _.extend({'startkey':_.flatten([detector.key,{}])},p)
    //                                var url_second =  source+'/'+view + '?'+geom_utils.toQuery(startkey);
    //                                var yr = detector.key[1]

    //                                if(detectors[yr] === undefined) detectors[yr]={}
    //                                if(detectors[yr][detector.key[0]]===undefined) detectors[yr][detector.key[0]]={'detector':postgis_utils.convertDetectorIdForSQL(detector.key[0])};
    //                                async.parallel([function(pcb){
    //                                                    superagent.get(url_first)
    //                                                    .end()
    //                                                           ,function(e,r,b){
    //                                                                if(e) return pcb(e)
    //                                                                var result = JSON.parse(b)
    //                                                                var start_date = result.rows[0].key.slice(1,4)
    //                                                                var start_hr = result.rows[0].key[4]
    //                                                                detectors[yr][detector.key[0]].startts = start_date.join('-')+' '+geom_utils.pad(start_hr)+':00';
    //                                                                return pcb();
    //                                                            });
    //                                                }
    //                                               ,function(pcb){
    //                                                    request(url_second
    //                                                           ,function(e,r,b){
    //                                                                if(e) return pcb(e)
    //                                                                var result = JSON.parse(b)
    //                                                                var stop_date = result.rows[0].key.slice(1,4)
    //                                                                var stop_hr = result.rows[0].key[4]
    //                                                                detectors[yr][detector.key[0]].stopts = stop_date.join('-')+' '+geom_utils.pad(stop_hr)+':00';
    //                                                                return pcb();
    //                                                            });
    //                                                }]
    //                                              ,function(err){
    //                                                   if(err) cb(err)
    //                                                   cb();
    //                                               })
    //                                return null;
    //                            }
    //                        }(source,view);
    // console.log(url)
    // request(url
    //        ,function(error,response,body){
    //             var detectorlist = JSON.parse(body).rows;
    //            if(detectorlist !== undefined && detectorlist.length > 0){
    //                // use a queue instaead of whilst
    //                var q = async.queue(min_max_detector, 5)
    //                q.push(detectorlist,function(err){
    //                    if(err) next(err)
    //                })
    //                q.drain = function(){
    //                    console.log('all detectors processed')
    //                    return next()
    //                }

    //            }else{
    //                next()
    //            }
    //            return null;
    //         });

}

exports.min_max_detectors = min_max_detectors

// so, now the calling function:
// 1. get all detectors, start and end times.  requires processing all
//    databases. processing can be async, but must complete before task 2
function populate_detectors(err,next){
    // // because d00 is on metis at the moment, store these as host, db pairs
    // var databases = [{'host':'metis.its.uci.edu'   ,'db':'/imputed%2fbreakup%2fd00'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd01'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd02'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd03'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd04'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd04%2f2009'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd05'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd05%2f2007'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd06'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd06%2f2009'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd07'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd08'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd09'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd10'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd11'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd12'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fwim'
    //                  ,'view':'_design/collect2/_view/idymdh_array'}
    //                 ];

    var databases = [{'host':chost,'db':'/imputed%2fcollated%2fd01%2f2007' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd01%2f2008' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd01%2f2009' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd02%2f2007' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd02%2f2008' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd02%2f2009' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd03%2f2007' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd03%2f2008' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd03%2f2009' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd04%2f2007' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd04%2f2008' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd04%2f2009' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd05%2f2007' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd05%2f2008' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd05%2f2009' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd06%2f2007' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd06%2f2008' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd06%2f2009' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd07%2f2007' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd07%2f2008' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd07%2f2009' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd08%2f2007' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd08%2f2008' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd08%2f2009' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd09%2f2007' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd09%2f2008' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd09%2f2009' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd10%2f2007' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd10%2f2008' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd10%2f2009' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd11%2f2007' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd11%2f2008' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd11%2f2009' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd12%2f2007' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd12%2f2008' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fd12%2f2009' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fwim%2f2007' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fwim%2f2008' ,'view':'_all_docs'}
                    ,{'host':chost,'db':'/imputed%2fcollated%2fwim%2f2009' ,'view':'_all_docs'}
                    ];

    async.forEach(databases
                  ,function(next_db,callback){
                      console.log('process '+JSON.stringify(next_db))
                      min_max_detectors(next_db,callback)
                      return null;
                  }
                  ,function(err){
                      if(err)
                          throw new Error(err)
                      console.log('done with processing all databases for detector ranges')
                      return next();
                  })

}

// and now moving on to
// 2. generate event times

function generate_event_times(err,next){

    // will process the global detectors for event times

    // group  detectors by start time
    var years = _.keys(detectors)
    var stacked_groups=[];
    _.each(years
          ,function(yr){

               var ds = _.values(detectors[yr])

               var detector_starting = _.groupBy(ds,'startts')
               var detector_stopping = _.groupBy(ds,'stopts')

               var start_times = _.keys(detector_starting)
               var stop_times  = _.keys(detector_stopping)

               // so groups of detectors come into play when the start timestamp
               // hits, and exit when the stopping timestamp hits

               var event_times = _.union(start_times,stop_times)
               event_times.sort()

               // iterate over the events, adding and removing detectors as needed.
               var active=[];

               var detector_groups = _.map(event_times
                                          ,function(t,idx){
                                               if(idx == event_times.length - 1){
                                                   return null;
                                               }
                                               if(detector_starting[t] !== undefined){
                                                   var newly_active = _.pluck(detector_starting[t]
                                                                             ,'detector')

                                                   active = _.union(active,newly_active)
                                               }
                                               if(detector_stopping[t] !== undefined){
                                                   var newly_inactive = _.pluck(detector_stopping[t]
                                                                               ,'detector')

                                                   active = _.difference(active,newly_inactive)
                                               }
                                               var nextts = event_times[idx+1]

                                               return {'ts':t,'nextts':nextts,'active':active}
                                           })
              stacked_groups = _.flatten([stacked_groups,detector_groups]);
           });
    return next(null,stacked_groups)

}


// 3. for each event time, generate components and maps code, and save it.
//    I could possibly split that up into freeways at a time
//    I could possibly run this in parallel, depending on load on DB

// for testing, dump freeway, ts, list of active detectors

function dump_results(err,detector_groups){
    console.log('output dump with '
               + detector_groups.length)
    _.each(detector_groups
          ,function(g){
               if(g != null){
                   console.log("('"+g.ts+"'::text)::timestamp without time zone as ts")
                   console.log("('",g.nextts+"'::text)::timestamp without time zone as endts")

               }
           });

}



async.nextTick(function(){
    populate_detectors(null
                      ,function(err){
                           if(err) throw new Error(err);
                           generate_event_times(null
                                               ,function(err, data){
                                                    if(err) throw new Error(err)
                                                    pg.connect(osmConnectionString,
                                                               do_inserts(data,function(err){
                                                                   if(err) {
                                                                       console.log(err);
                                                                       throw new Error();
                                                                   }
                                                                   console.log('all done')
                                                                   pg.end()
                                                                   return null;
                                                               })
                                                                   );
                                                });
                       });
});



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
                        });
                  q1.on('end'
                        ,function(result){
                            console.log('done with         insert '+[startts,stopts].join(' to '))
                        });
                  var q2 = client.query(related_insert.join(' '))
                  q2.on('error'
                        ,function(err){
                            if(err){
                                console.log("insert related error" + JSON.stringify(err));
                                console.log(related_insert.join('\n'));
                                return next('die');
                            }
                        });
                  q2.on('end'
                        ,function(result){
                            console.log('done with related insert '+[startts,stopts].join(' to '))
                            return null;
                        });
                  return null;
              });
        client.on('drain', function() {
            console.log("drained all queries");
            client.end()
            return next(null)
        });
        console.log('done queueing queries')
        return null;
    };
}
