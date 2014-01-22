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

var options = {};

var env = process.env;
var cuser = options.user || env.COUCHDB_USER ;
var cpass = options.pass || env.COUCHDB_PASS ;
var chost = options.host || env.COUCHDB_HOST ;
var cport = options.port || env.COUCHDB_PORT || 5984;
var trackerdbname = options.trackerdbname || env.COUCHDB_TRACKINGDB || 'vdsdata%2ftracking';

var couch = 'http://'+chost+':'+cport;
var tracker = couch +'/'+trackerdbname;

var pg_handler = require('./pg_inserts').pg_handler
var hit_pg = pg_handler()

var databases = []

var prefix = process.env.COUCH_CACHE_PREFIX || 'imputed%2fcollated'

function get_relevant_databases(next){
    var uri = couch+'/_all_dbs'
    superagent.get(uri)
    .set('accept','application/json')
    .end(function(err,res){
        var re = /%2f/gi;
        var unescaped_prefix = prefix.replace(re, "/");

        var matcher = new RegExp('^'+unescaped_prefix)
        databases =
            _.filter(res.body,function(db){
                return matcher.test(db)
            })
        next(null,databases)
    })
}

// steps:
// 1. get all detectors, start and end times.  requires processing all
//    databases. processing can be async, but must complete before task 2
// 2. generate event times
// 3. for each event time, generate components and maps code, and save it.
//    I could possibly split that up into freeways at a time
//    I could possibly run this in parallel, depending on load on DB
//



// 3. for each event time, generate components and maps code, and save it.
//    I could possibly split that up into freeways at a time
//    I could possibly run this in parallel, depending on load on DB

// for testing, dump freeway, ts, list of active detectors

function dump_results(data,cb){
    console.log('output dump with '
               + data.length)
    _.each(data
          ,function(g){
               if(g != null){
                   console.log("('"+g.ts+"'::text)::timestamp without time zone as ts")
                   console.log("('",g.nextts+"'::text)::timestamp without time zone as endts")

               }
           });
    return cb()

}

var generate_event_times = require('./generate_event_times')
var walker = require('./walk_through_detectors').walker
var utils=require('./utils')

function do_it(){

    var detectors={'2007':{}
                  ,'2009':{}
                  ,'2008':{}
                  };

    function stash_detectors(year,cb){
        return function(e,dctrs){
            if(e) return cb(e)
            _.each(dctrs,function(d){
                if(d.detector !== undefined){
                    var did  = d.detector
                    if(detectors[year][did]===undefined){
                        var pg_did = utils.convertDetectorIdForSQL(did)
                        detectors[year][did]={'detector':pg_did}
                    }
                    detectors[year][did].startts = d.start
                    detectors[year][did].stopts = d.end
                }
                return null
            });
            return cb()
        }
    }
    async.waterfall([get_relevant_databases
                    // ,function(dbs,cb){
                    //      console.log('testing, trim dbs')
                    //      var dbfewer = dbs.slice(0,10)
                    //      dbfewer.push(dbs.pop())
                    //      cb(null,dbfewer)
                    //  }
                    ,function(dbs,cb){
                         console.log(dbs)
                         async.eachLimit(dbs,1
                                     ,function(db,cb2){
                                          var parts = db.split('/')
                                          var year = parts.pop()
                                          if(detectors[year] === undefined)
                                              detectors[year]={}
                                          var district = parts.pop()
                                          console.log(db)
                                          walker({district:district
                                                 ,year:year}
                                                ,stash_detectors(year,cb2))
                                          return null
                                      }
                                     ,function(e){
                                          return cb(e)
                                      })
                         return null
                     }
                    ,function(cb){
                         // got min maxes, now handle
                         generate_event_times(detectors,cb)
                         return null
                     }
                    //,dump_results
                    ,hit_pg
                    ]
                   ,function(e){
                        if(e) {
                            console.log('got an error')
                            console.log(e)
                            throw new Error(e)
                        }
                        console.log('all done')
                        return null
                    })
    return null
}


async.nextTick(function(){
    return do_it()
});
