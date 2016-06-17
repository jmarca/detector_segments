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

var _ = require('lodash')
var querystring = require('querystring')
var queue = require('d3-queue').queue

var argv = require('minimist')(process.argv.slice(2))
var path = require('path')


var pg_handler = require('./pg_inserts').pg_handler
// this should be based on config?

var databases = []

var path = require('path');
var rootdir = path.normalize(__dirname)

var detector_fetch = require('./detector_fetch.js')

var config_okay = require('config_okay')
var config_file
if(argv.config === undefined){
    config_file = path.normalize(rootdir+'/../config.json')
}else{
    config_file = path.normalize(rootdir+'/../'+argv.config)
}
console.log('setting configuration file to ',config_file,'.  Change with the --config option.')

var debug = false

var num_CPUs = require('os').cpus().length;
// debugging
num_CPUs = 1


// steps:
// 1. get detectors that are valid for this year
// 4. use start stop times to generate lists of effective/active detectors
//
// 5. for each unique list (which has a start time and the stop time
//    is the next start time) do stuff
//
//    1. get all of the detectors active at each timestamp
//
//    2. run the super long sql statement to save segments to psql
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

    var year
    if(argv.year !== undefined){
        year = argv.year
    }else{
        if(config.year !== undefined){
            year = config.year
        }
    }
    if(!year){
        console.log('pass year in using the --year argument')
        return null
    }
    year = +year // make sure it is numeric
    config.couchdb.db=config.couchdb.trackingdb
    detector_fetch(year,config,function(err,detectors){
        if(err) throw new Error(err)
        var secondloop = queue(1)
        // got detectors, now handle
        var keys = Object.keys(detectors)

        // debugging, just do one freeway_dir
        // keys = [keys[2]]

        keys.forEach(function(freeway_dir){
            var group = detectors[freeway_dir]
            // generate events from the group
            var f = _.uniq(_.map(group,'freeway'))[0]
            var d = _.uniq(_.map(group,'direction'))[0]
            if(f+'-'+d !== freeway_dir){
                console.log(freeway_dir,f,d,detectors[freeway_dir])
                throw new Error('freeway dir mismatch?')
            }

            var events = generate_event_times(group)
            // need to verify that new events is same as old, also
            // modify code as necessary
            secondloop.defer(pg_handler,config,events,f,d)
            return null
        })
        secondloop.await(function(e,r){
            if(e) throw new Error(e)
            pg_handler.done()
            return null
        })

        return null
    })
    return null
}

config_okay(config_file,function(e,c){
    if(c.debug){
        debug = true
    }
    return do_it(null,c)
})
