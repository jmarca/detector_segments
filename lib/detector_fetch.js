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
