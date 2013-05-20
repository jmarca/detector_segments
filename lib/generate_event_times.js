var _ = require('lodash')
function generate_event_times(detectors,next){

    // will process the global detectors for event times

    // group  detectors by start time
    var years = _.keys(detectors)
    var stacked_groups=[];
    _.each(years
          ,function(yr){

               var ds = _.values(detectors[yr])

               var detector_starting = _.groupBy(ds,'startts')
               var detector_stopping = _.groupBy(ds,'stopts')

               var start_times = _.keys(detector_starting)
               var stop_times  = _.keys(detector_stopping)

               // so groups of detectors come into play when the start timestamp
               // hits, and exit when the stopping timestamp hits

               var event_times = _.union(start_times,stop_times)
               event_times.sort()

               // iterate over the events, adding and removing detectors as needed.
               var active=[];

               var detector_groups = _.map(event_times
                                          ,function(t,idx){
                                               if(idx == event_times.length - 1){
                                                   return null;
                                               }
                                               if(detector_starting[t] !== undefined){
                                                   var newly_active = _.pluck(detector_starting[t]
                                                                             ,'detector')

                                                   active = _.union(active,newly_active)
                                               }
                                               if(detector_stopping[t] !== undefined){
                                                   var newly_inactive = _.pluck(detector_stopping[t]
                                                                               ,'detector')

                                                   active = _.difference(active,newly_inactive)
                                               }
                                               var nextts = event_times[idx+1]

                                               return {'ts':t,'nextts':nextts,'active':active}
                                           })
              stacked_groups = _.flatten([stacked_groups,detector_groups]);
           });
    return next(null,stacked_groups)

}

module.exports=generate_event_times
