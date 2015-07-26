// psql stuff
var pg = require('pg');
var _=require('lodash')

var group_handler = require('./group_handler.js')

function setup_connection(opts){

    var host = opts.postgresql.host ? opts.postgresql.host : '127.0.0.1';
    var user = opts.postgresql.auth.username ? opts.postgresql.auth.username : 'myname';
    //var pass = opts.postgresql.auth.password ? opts.postgresql.password : '';
    var port = opts.postgresql.port ? opts.postgresql.port :  5432;
    var db  = opts.postgresql.db ? opts.postgresql.db : 'spatialvds'
    var connectionString = "pg://"+user+"@"+host+":"+port+"/"+db
    console.log(connectionString)
    return connectionString
}

var queue = require('queue-async')

function all_done(){
    return pg.end()
}

function pg_handler(options,data,freeway,direction,callback){
    // debugging
    // data = [data[0]]
    var connectionString = setup_connection(options)
    var q = queue(1)
    pg.connect(connectionString
               ,function(err,client,clientdone){

                   if(err){
                       console.log(err)
                       return callback(err)
                   }
                   data.forEach(function(group){
                       q.defer(group_handler,group,freeway,direction,client)
                   })
                   q.await(function(e,r){
                       if(e){
                           console.log('a query failed')
                           clientdone()
                           return callback(e)
                       }
                       console.log('client finished normally')
                       // finished normally
                       clientdone()
                       callback()
                       return null
                   })
                   return null
               })
    return null
}
pg_handler.done = all_done

module.exports.pg_handler=pg_handler
