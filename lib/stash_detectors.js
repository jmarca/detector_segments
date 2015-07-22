var detectors = {}

function stash_detectors(year){
    if(detectors[year] === undefined){
        detectors[year]={}
    }
    return function(dctrs){
        dctrs.forEach(function(d){
            if(d.detector !== undefined){

                /** fixme **/
                /** I am not sure I want to keep the year to year fiction going **/

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
    }
}
