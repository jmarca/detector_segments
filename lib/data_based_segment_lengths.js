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

var request = require('request')
var _ = require('lodash')
var querystring = require('querystring')
var queue = require('queue-async')

var geom_utils=require('geom_utils')
var env = process.env;

var pg_handler = require('./pg_inserts').pg_handler

// this should be based on config?
// var hit_pg = pg_handler()

var databases = []

var path = require('path');
var rootdir = path.normalize(__dirname)

var config_okay = require('config_okay')
var config_file = env.CONFIG_FILE || path.normalize(rootdir+'/../config.json')
var config

function _configure(cb){
    if(config === undefined){
        config_okay(config_file,function(e,c){
            config = c
            return cb(null,config)
        })
        return null
    }else{
        return cb(null,config)
    }
}



// steps:
// 1. get VDS detectors that are valid for this year
// 2. get WIM detectors that are valid for this year
// 3. Does each WIM station have ABSPM and freeway too?
// 4. use start stop times to generate lists of effective/active detectors
//
// 5. for each unique list (which has a start time and the stop time
//    is the next start time) do the following loop:
//
//    1. Use lists of WIM and VDS to get geo data from psql.  Query
//       needs to generate segment lengths for each detector, from prior
//       to next.
//
//    2. save imputed data and the segment length for each detector
//
// for each year, each freeway, etc etc.

// useful dump function for testing
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
        if(detectors[year] === undefined){
            detectors[year]={}
        }
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
    async.waterfall([config_okay
                    ,configure
                    ,get_relevant_databases
                    // ,function(dbs,cb){
                    //      console.log('testing, trim dbs')
                    //      var dbfewer = dbs.slice(0,10)
                    //      dbfewer.push(dbs.pop())
                    //      cb(null,dbfewer)
                    //  }
                    ,function(task,cb){
                         var dbs = task.couchdb.databases
                         console.log(dbs)
                         async.eachLimit(dbs,1
                                     ,function(db,cb2){
                                          var parts = db.split('/')
                                          var year = parts.pop()
                                          if(detectors[year] === undefined)
                                              detectors[year]={}
                                          var district = parts.pop()
                                          console.log(db)
                                          var db_task = _.clone(task)
                                          db_task.district=district
                                          db_task.year=year
                                          walker(db_task
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
