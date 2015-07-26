var _ = require('lodash')

/**
 * generate_event_times
 *
 * given a group for a particular year, highway, direction, with each
 * row of the group a hash containing at least mints, maxts; generate
 * a new array in which each element is a group with active detectors.
 *
 * for example:
 * if you have some detectors a, b, c, and d, and say you know the
 * "active" ties for each detector in records like:
 *
 * {id:a,mints:t1,maxts:t4}
 * {id:b,mints:t5,maxts:t6}
 * {id:c,mints:t1,maxts:t3}
 * {id:d,mints:t3,maxts:t5}
 *   ...
 *
 * Which means detector "a" is up and running from t1 through t4, etc.
 *
 * Then combining these will make an array of "active" detectors
 * versus timestamp like so:
 *
 *        a b c d e f g h i j k l m
 *  t1    1   1     1
 *  t2    1   1   1 1 1   1   1
 *  t3    1   1 1 1 1 1 1 1 1 1
 *  t4    1     1 1 1 1 1   1 1
 *  t5      1   1 1 1   1   1   1 1
 *  t6      1     1 1   1       1 1
 *
 *
 * The idea is to look at this edge-on (by time) to get arrays of:
 *
 * {time:t1,detectors:[a,c,f]}
 * {time:t2,detectors:[a,c,e,f,g,i,k]}
 * {time:t3,detectors:[a,c,d,e,f,g,h,i,j,k]}
 *   ...
 *
 * These are then used to define the segment lengths.  So at time t1,
 * detector "c" has a segment that is from "a" to "f", but then at
 * time t2 the end-point of that segment changes to "e", because "e"
 * is now active.  The segment triplets are thus [a,c,f] for all
 * imputations from t1 through t2 and then [a,c,e] for imputations
 * from t2 through t3 (when "d" becomes active)
 *
 * So for each time stamp, you have a triplet list of active
 * detectors, so you can determine the effective segment lengths
 * between each detector along the length of the freeway.  Of course,
 * other code is used to compute these distances.
 *
 * @param {Object[]} group - The employees who are responsible for the project.
 * @param {string} group[].mints - The minimum active time for truck imputations.
 * @param {string} group[].maxts - The maximum active time for truck imputations.
 * @param {string} group[].id    - The detector id, either wim or vds.
 */
function generate_event_times(group){
    // simple little state machine.
    // detectors transition from holding, to active, to inactive
    var mintimes = []
    var maxtimes = []
    // parse min and max times into Dates

    // issue here is that max times are *active*  I need the next hour
 // also, be careful to not trip over stupid local time stuff by
 // forcing all parsing to be UTC, and output to be UTC, and to strip
 // off the .000Z at the end of the output string.  This gives a
 // pretty accurate increment the hour by one
    _.each(group,function(row){
        mintimes.push(row.mints)
        // fiddle with the max ts to be one hour later, so as to
        // represent the time at which the detector is NOT active,
        // rather than the last time at which the detector IS active
        var ets = new Date( row.maxts + ' UTC')
        ets.setHours(ets.getHours()+1)
        ets = ets.toISOString().slice(0,-5) // drop .000Z
        ets = ets.replace(/T/,' ')
        maxtimes.push(ets)
        return null
    })
    var times = _.union(mintimes,maxtimes)
    times = times.sort()
    // console.log(times)
    var pending = _.clone(group,true)
    var active = []
    var states = []
    // this is inefficient in that I am pushing rows around, but it is
    // clearer than pushing pointers, and really a few instructions
    // aren't going to be noticeable here

    times.forEach(function(time,idx){
        // get rid of the T in the middle

        // remove from active all site that have expired
        // console.log('before pending',active.length)
        active = active.filter(function(row){
            return (row.maxts > time)
        })
        // console.log('after filter',active.length)

        // move from pending to active
        var next_pending = []
        pending.forEach(function(row){
            if(row.mints == time){
                active.push(row)
            }else{
                next_pending.push(row)
            }
        })
        // console.log('after pending',active.length)
        // for next iteration
        pending = next_pending

        // dump current active
        var detectors = (_.pluck(active,'id')).sort()
        if(detectors.length>0){

            states.push({
                'ts':time,
                'nextts':times[idx+1],
                'detectors': detectors
            })
        }

    })
    return states
}

module.exports=generate_event_times
