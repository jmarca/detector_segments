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

var databases = []

var path = require('path');
var rootdir = path.normalize(__dirname)

var detector_fetch = require('./detector_fetch.js')

var config_okay = require('config_okay')
var config_file = env.CONFIG_FILE || path.normalize(rootdir+'/../config.json')
var debug = false



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
    if(debug){
        console.log('debugging dump')
        data.forEach(function(g){
            if(g != null){
                console.log("('"+g.ts+"'::text)::timestamp without time zone as ts")
                console.log("('",g.nextts+"'::text)::timestamp without time zone as endts")

            }
            return null
        })
    }
    return cb()

}

var utils=require('./utils')
var generate_event_times = require('./events.js')

function do_it(err,config){

    // var hit_pg = pg_handler(config)

    var years = config.years
    if(years.length === 0){
        throw new Error('try setting years array in config file')
    }
    var firstloop = queue(1)
    years.forEach(function(year){
        firstloop.defer(detector_fetch,year,config)
    })
    var secondloop = queue(1)
    firstloop.awaitAll(function(err,detectors){
        if(err) throw new Error(err)
        // got detectors, now handle
        years.forEach(function(year,idx){
            var grouped_by_freeway = detectors[idx]
            // debug
            grouped_by_freeway = [grouped_by_freeway[0]]
            _.each(grouped_by_freeway,function(group,freeway_dir){
                // generate events from the group
                console.log('freeway dir is '+freeway_dir)
                var events = generate_event_times(group)
                console.log(events)
                // need to verify that new events is same as old, also
                // modify code as necessary
                secondloop.defer(pg_handler,config,events)
                return null
            })
            return null
        })
        secondloop.await(function(e,r){
            if(e) throw new Error(e)
            return null
        })
        return null
    })
}

config_okay(config_file,function(e,c){
    if(c.debug){
        debug = true
    }
    return do_it(null,c)
})
