/* global require console process Buffer JSON */

// purpose.  Get the first and last times in each year for data from WIM sites
// push those start and stop times to CouchDB, so that I can get
// active detectors per freeway anytime

// access WIM data by hitting postgres server with this query for each site for each year

// select  min(ts),max(ts) from wim_data where ts between '2007-01-01 00:00:00' and '2008-01-01 00:00:00' and site_no = site...

// use this query to get all sites

// select distinct site_no, direction from wim_lane_dir order by site_no, direction;


var request = require('request');
var _ = require('underscore');
var querystring = require('querystring');
var async = require('async');

var pg = require('pg').native

var date_formatter = require('date-functions')

var options = {};
// eventually use optimist for options here

var env = process.env;
var cuser = options.user || env.COUCHDB_USER ;
var cpass = options.pass || env.COUCHDB_PASS ;
var chost = options.host || env.COUCHDB_HOST ;
var cport = options.port || env.COUCHDB_PORT || 5984;
var trackerdbname = options.trackerdbname || env.COUCHDB_TRACKINGDB || 'vdsdata%2ftracking';

var couch = 'http://'+chost+':'+cport;
var tracker = couch +'/'+trackerdbname;


// psql stuff
var host = options.host     ? options.host     :  env.PSQL_HOST || '127.0.0.1';
var user = options.username ? options.username :  env.PSQL_USER;
var pass = options.password ? options.password :  env.PSQL_PASS;
var port = options.port ? options.port :  5432;
var osmConnectionString        = "pg://"+user+":"+pass+"@"+host+":"+port+"/osm";
var spatialvdsConnectionString        = "pg://"+user+":"+pass+"@"+host+":"+port+"/spatialvds";

console.log( osmConnectionString + ' ' + spatialvdsConnectionString)
// set up one connecton to pick off WIM sites
var wim_client;
// set up another connection for min max time queries
var minmax_client;
var minmax_name  = 'minmax'
// back the initial query up by one year
var minmax_query = "select extract( year from (min(ts)) ) as year, extract( epoch from (min(ts))) as mints, extract( epoch from (max(ts))) as maxts from wim_data where ts between $1 and $2 and site_no=$3"


var min_name  = 'minq'
var min_query = "select  extract( year from (min(ts)) ) - 1 as year from wim_data where site_no=$1"
async.parallel([function(cb){
                    pg.connect(spatialvdsConnectionString
                                 ,function(err,client){
                                   if(err)
                                       throw new Error(err)
                                      wim_client = client
                                      minmax_client = client
                                      // prepare statement
                                      minmax_client.query({'name':min_name
                                                          ,'text':min_query
                                                          ,'values':[1]}
                                                         ,function(e,r){
                                                              console.log(e)
                                                              console.log(r)
                                                          })
                                      minmax_client.query({'name':minmax_name
                                                          ,'text':minmax_query
                                                          ,'values':['2007-01-01','2007-01-02',1]}
                                                         ,function(e,r){
                                                              console.log(e)
                                                              console.log(r)
                                                          })

                                      cb()
                                  })
                }
               // ,function(cb){
               //      pg.connect(spatialvdsConnectionString
               //                ,function(err,client){
               //                     if(err)
               //                         throw new Error(err)
               //                        minmax_client = client
               //                        // prepare statement
               //                        minmax_client.query({'name':min_name
               //                                            ,'text':min_query
               //                                            ,'values':[1]}
               //                                           ,function(e,r){
               //                                                console.log(e)
               //                                                console.log(r)
               //                                            })
               //                        minmax_client.query({'name':minmax_name
               //                                            ,'text':minmax_query
               //                                            ,'values':['2007-01-01','2007-01-02',1]}
               //                                           ,function(e,r){
               //                                                console.log(e)
               //                                                console.log(r)
               //                                            })
               //                        cb()
               //                    })
               //  }
               ]
              ,function(err){
                   if(err) throw new Error(err)
                   get_wim_sites()
               })


var couch_db = ['vdsdata','tracking'].join('%2f')
var wim_sites = {}

function dump_to_couchdb(_id){
    console.log(wim_sites[_id])

    // add the yearly min max data times to the correct wim tracking doc

    var uri = couch+'/'+couch_db+'/'+_id
    var opts =  {'uri':uri
                ,'method': 'GET'
                , 'headers': {}
                }
    opts.headers.authorization = 'Basic ' + new Buffer(cuser + ':' + cpass).toString('base64');
    opts.headers['Content-Type'] =  'application/json';

    request(opts
           ,function(e,r,b){
                if(e) throw new Error(e);
                // now add the revisions to the docs and save updates

                var doc = JSON.parse(b)
                // interleave the year data from the database
                _.each(wim_sites[_id]
                      ,function(year_min_max){
                           doc[year_min_max.year].data_range = {'startts':year_min_max.startts
                                                               ,'endts':year_min_max.endts}
                       });
                // now save the revised document
                opts.body = JSON.stringify(doc);
                opts.method = 'PUT'
                request(opts
                       ,function(e,r,b){
                            // do something here when the put is done
                            // try cleaning up maybe
                            if(e){ console.log('bulk doc save error '+e)
                                   throw new Error(e)
                                   if(r.status !== 200)
                                       throw new Error(e)
                                 }
                            return null;
                        });
                return null;
            });
    return null;
}

function get_minmax(options,cb){
    var site_no = options.site_no
    var _id = options._id
    wim_sites[_id] = []
    return function(e,r){
        if(e){console.log(e)
                throw new Error(e)
               }
        if(r.rows === undefined
         || r.rows.length == 0 )
            return null
        var mints = new Date(1000 * r.rows[0].mints)
        var maxts = new Date(1000 * r.rows[0].maxts)

        var format ="Y-m-d H:i:s"
        var year = /(\d{4})/.exec(mints)[1]
        wim_sites[_id].push({'year':year
                            ,'startts': mints.dateFormat(format)
                            ,'stopts':  maxts.dateFormat(format)})
        return null
    }
}

function next_query_handler(site_no,cb){
    return function(e,r){
        console.log('next query handler')
        if(e){console.log(e)
                throw new Error(e)
               }
        if(r.rows === undefined
         || r.rows.length == 0 )
            return null
        var year = r.rows[0].year
        // if(+year == 2008) return null  // testing abort
        if(+year == 2009) return null  // I don't currently have data past 2009
        year = +year +1
        var a = year + '-01-01 00:00:00';
        year = +year +1
        var b = year + '-01-01 00:00:00';
        return {'name':minmax_name
               ,'values':[a,b,site_no]
               }
    }
}

function get_wim_sites(){

        // for each detector, fire off min year thing

    var q1 = wim_client.query("select distinct site_no, direction from wim_lane_dir order by site_no, direction limit 1")

    // fix me when done drop the limit

    q1.on('row'
         ,function(row){
              var _id = ['wim',row.site_no,row.direction].join('.')
              var site_no = row.site_no
              var mm_handler = get_minmax({'site_no':site_no
                                       ,'_id':_id})
              var next_handler = next_query_handler(site_no)

              var handler = function(e,r){
                      if(e) throw new Error(e)
                      console.log('tick '+ _id)
                      async.parallel([function(cb){
                                          mm_handler(e,r)
                                          cb()
                                      }
                                     ,function(cb){
                                          var options = next_handler(e,r)
                                          if(options !== undefined){
                                              console.log(options)
                                              minmax_client.query(options,handler)
                                          }else{
                                              dump_to_couchdb(_id)
                                          }
                                          cb()
                                      }]
                                    )
                  };


              // first get min, then get min maxes iteratively
              minmax_client.query({'name':min_name
                                  ,'values':[site_no]}
                                 ,function(e,r){
                                      console.log('handling '+_id)

                                      var options = next_handler(e,r)
                                      console.log(options)
                                      if(options !== undefined)
                                          minmax_client.query(options,handler)
                                  })
          })

}
