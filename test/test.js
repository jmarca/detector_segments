/* global require console process it describe after before */

var should = require('should')
var _ = require('lodash');
// eventually, work out how to do
// var rewire = require("rewire");
// // rewire acts exactly like require.
// var myModule = rewire("../lib/parse_pat_reports");

var ppr = require('../lib/parse_pat_reports')

// test db
var pg = require('pg'); //native libpq bindings = `var pg = require('pg').native`
var env = process.env
var puser = process.env.PSQL_USER
var ppass = process.env.PSQL_PASS
var phost = process.env.PSQL_HOST || '127.0.0.1'
var pport = process.env.PSQL_PORT || 5432
var pdbname = process.env.PSQL_DB || 'test'
var connectionString = "pg://"+puser+":"+ppass+"@"+phost+":"+pport+"/"+pdbname;

describe('min_max_detectors',function(){
    it('should do something',function(done){
        var databases = [{'host':'metis.its.uci.edu'   ,'db':'/imputed%2fbreakup%2fd00'}
                        ,{'host':'lysithia.its.uci.edu','db':'/imputed%2fbreakup%2fd04%2f2009'}
                        ];
        dbsl.min_max_detectors(databases[0],function(err){
            should.not.exist(err)

    })
})