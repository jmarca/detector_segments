var couch_get_view = require('couchdb_get_views')
var async = require('async')

var _prefix = process.env.COUCH_CACHE_PREFIX || 'imputed%2fcollated'

function walker(opts,next){

    var district = opts.district
    var db_year = opts.year
    var query_year = +db_year+1
    var prefix = opts.couchdb.prefix || _prefix
    var couch_database = [prefix,district,db_year].join('%2f')

    function next_detector(previous_detector,cb){
        var startkey = previous_detector+'-'+query_year
        var query = {'startkey':startkey
                    ,'limit':1
                    ,'descending':false
                    ,'db':couch_database
                    ,'view':'_all_docs'
                    }
        var ddoc_test = /_design/;
        couch_get_view(query,function(e,docs){
            // docs.rows[0] should not be empty. if it is, done
            if(e) return cb(e)
            var d,start
            if(docs.rows !== undefined
             && docs.rows[0] !== undefined
             && docs.rows[0].key !== undefined
             && !ddoc_test.test(docs.rows[0].key)){
                var k = docs.rows[0].key
                var key_parts = k.split('-')
                d = key_parts.shift()
                start = key_parts.join('-')
            }
            return cb(null,{'detector':d
                             ,'start':start})
        })
        return null
    }
    function last_time(current_detector,cb){
        var startkey = current_detector+'-'+query_year
        var query = {'startkey':startkey
                    ,'limit':1
                    ,'descending':true
                    ,'db':couch_database
                    ,'view':'_all_docs'
                    }
        couch_get_view(query,function(e,docs){
            // docs.rows[0] should never be empty. if it is, throw
            if(e) return cb(e)
            var d,end
            if(docs.rows !== undefined
             && docs.rows[0] !== undefined
             && docs.rows[0].key !== undefined){
                var k = docs.rows[0].key
                var key_parts = k.split('-')
                d = key_parts.shift()
                end = key_parts.join('-')
            }else{
                throw new Error('no docs for last time stamp.  query is '+JSON.stringify(query) + ' return value is '+JSON.stringify(docs))
            }
            return cb(null,{'detector':d
                             ,'end':end})
        })
        return null
    }

    // starting with detector '', iterate over next_detector, last time until done
    var detectors = {}
    var current_detector = ''
    var keep_going = true
    async.whilst(function(){
        return keep_going
    }
                ,function(cb){
                     async.waterfall([function(cb2){
                                          return next_detector(current_detector,cb2)
                                      }
                                     ,function(state,cb2){
                                          if(state.detector === undefined || !state.detector){
                                              keep_going = false
                                              return cb2('done')
                                          }
                                          current_detector = state.detector
                                          detectors[current_detector]={'detector':state.detector
                                                                    ,'start':state.start}
                                          last_time(current_detector,cb2)
                                          return null
                                      }
                                     ,function(state,cb2){
                                          detectors[current_detector].end=state.end
                                          cb2(null)
                                      }
                                     ]
                                    ,function(e){
                                         if(e){
                                             if(e!=='done')
                                                 return cb(e)
                                         }
                                         return cb()

                                     })

                 }
                ,function(e){
                     console.log('done with whilst')
                     if(e){
                         console.log(e)
                         return next(e)
                     }
                     next(null,detectors)
                     return null
                 })
    return null
}
// deprecated
//exports.walker = walker
