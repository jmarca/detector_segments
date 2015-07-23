// get VDS start and end times from tracking database
// get WIM start and end times from tracking database


                    ,configure
                    ,get_relevant_databases
                    // ,function(dbs,cb){
                    //      console.log('testing, trim dbs')
                    //      var dbfewer = dbs.slice(0,10)
                    //      dbfewer.push(dbs.pop())
                    //      cb(null,dbfewer)
                    //  }
                    ,function(task,cb){
                         var dbs = task.couchdb.databases
                         console.log(dbs)
                         async.eachLimit(dbs,1
                                     ,function(db,cb2){
                                          var parts = db.split('/')
                                          var year = parts.pop()
                                          if(detectors[year] === undefined)
                                              detectors[year]={}
                                          var district = parts.pop()
                                          console.log(db)
                                          var db_task = _.clone(task)
                                          db_task.district=district
                                          db_task.year=year
                                          walker(db_task
                                                ,stash_detectors(year,cb2))
                                          return null
                                      }
                                     ,function(e){
                                          return cb(e)
                                      })
                         return null
                     }
