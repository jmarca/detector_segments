var viewer = require('couchdb_get_views')
function set_couchdb_options(opts){
    var o = {}
    if(opts.config_file !== undefined){
        o.config_file = opts.config_file
        return o
    }
    if(opts.couchdb !== undefined){
        Object.keys(opts.couchdb).forEach(function(k){
            o[k] = opts.couchdb[k]
        })
        return o
    }
    return o
}

function query_view(opts,cb){

    var o = set_couchdb_options(opts)
    o['view']='_design/truckimputed/_view/mints_maxts'
    o['reduce']=false
    viewer(o
          ,function(err,docs){
               if(err) throw new Error(err)
               return cb(null,docs)
           })
    return null
}

/**
 * This callback type is called `detector_fetch_callback` and is
 * displayed as a global symbol.
 *
 * @callback detector_fetch_callback
 * @param {string} error, if any
 * @param {Object[]} groups that has grouped time data stuff
 * @param {string} group[].mints - The minimum active time for truck imputations.
 * @param {string} group[].maxts - The maximum active time for truck imputations.
 * @param {string} group[].id    - The detector id, either wim or vds.
 * @param {number} group[].freeway   - The freeway number
 * @param {string} group[].direction - The direction of travel (N,S,E, or W)
 */

/**
 * detector_fetch
 *
 *
 * @param {number} year
 * @param {Object} opts
 * @callback detector_fetch_callback cb
 * @returns {NULL}
 *
 */
function detector_fetch(year,opts,cb){
    // hit couchdb to get the view with the detectors

    // yo

    opts.couchdb.startkey=[+opts.year]
    opts.couchdb.endkey=[+opts.year,10000] // there are no such freeways

    query_view(opts,function(e,docs){

        if(docs.rows === undefined || docs.rows.length === 0){
            return cb() // nothing to do
        }

        // group according to the freeway,direction
        var groups = {}
        docs.rows.forEach(function(row,i){
            var freeway_dir = row.key[1] + '-' + row.key[2]
            if(groups[freeway_dir] === undefined){
                groups[freeway_dir]=[]
            }
            groups[freeway_dir].push({
                'freeway':row.key[1]
                ,'direction':row.key[2]
                ,'mints':row.key[3]
                ,'maxts':row.key[4]
                ,'id':row.id
            })
            return null
        })
        return cb(e,groups)
    })
    return null
}
module.exports=detector_fetch
