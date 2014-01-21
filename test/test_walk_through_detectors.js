var walker = require ('../lib/walk_through_detectors').walker
var should = require('should')
var _ = require('lodash')

describe('walker',function(){
    it('should walk through district 6 2007',function(done){
        walker({district:'d06'
               ,year:2007
               }
              ,function(e,detectors){
                   should.exist(detectors)
                   should.not.exist(e)
                   var l = _.keys(detectors).length
                   l.should.eql(14)
                   _.each(detectors,function(v,k){
                       v.should.have.property('detector')
                       v.should.have.property('start')
                       v.should.have.property('end')
                   });
                   done()
                   return null
               })
    })
    it('should walk through district 10 2009',function(done){
        walker({district:'d10'
               ,year:2009
               }
              ,function(e,detectors){
                   should.exist(detectors)
                   should.not.exist(e)
                   var l = _.keys(detectors).length
                   l.should.eql(283)
                   _.each(detectors,function(v,k){
                       v.should.have.property('detector')
                       v.should.have.property('start')
                       v.should.have.property('end')
                   });
                   done()
                   return null
               })
    })
})