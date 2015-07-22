// psql stuff
var pg = require('pg');
var _=require('lodash')

var group_handler = require('./group_handler.js')

function setup_connection(opts){

    var host = opts.postgresql.host ? opts.postgresql.host : '127.0.0.1';
    var user = opts.postgresql.auth.username ? opts.postgresql.auth.username : 'myname';
    //var pass = opts.postgresql.auth.password ? opts.postgresql.password : '';
    var port = opts.postgresql.port ? opts.postgresql.port :  5432;
    var db  = opts.postgresql.grid_merge_sqlquery_db ? opts.postgresql.grid_merge_sqlquery_db : 'spatialvds'
    var connectionString = "pg://"+user+"@"+host+":"+port+"/"+db
    return connectionString
}

var queue = require('queue-async')

function pg_handler(options,data,callback){

    var connectionString = setup_connection(options)
    var q = queue(1)
    pg.connect(connectionString
               ,function(err,client,clientdone){
                   if(err) return callback(err)
                   data.forEach(function(group){
                       q.defer(group_handler,group,client)
                   })
                   q.await(function(e,r){
                       if(e){
                           console.log('a query failed')
                           clientdone()
                           return callback(e)
                       }
                       // finished normally
                       clientdone()
                       callback()
                       return null
                   })
                   return null
               })
    return null
}


module.exports.pg_handler=pg_handler
