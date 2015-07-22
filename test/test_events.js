var should = require('should')
var generate_event_times = require('../lib/events.js')

describe('generate event times',function(){
    it('should not crash on empty list',function(){
        var result = generate_event_times([])
        should.exist(result)
        result.should.be.instanceOf(Array)
        result.should.have.lengthOf(0)
        return null
    })

    it('should process a list of events properly',function(){

        var group = [
            {
                'id':'A',
                'mints':'2012-09-06 14:00:00', //
                'maxts':'2012-12-28 09:00:00'  //
            },
            {
                'id':'B',
                'mints':'2012-09-16 03:00:00', //
                'maxts':'2012-12-28 09:00:00'  //
            },
            {
                'id':'C',
                'mints':'2012-09-06 16:00:00', //
                'maxts':'2012-09-12 00:00:00'  //
            },
            {
                'id':'D',
                'mints':'2012-09-12 14:00:00', //
                'maxts':'2012-12-28 09:00:00'  //
            },
            {
                'id':'E',
                'mints':'2012-10-29 02:00:00', //
                'maxts':'2012-12-28 18:00:00'
            }
        ]
        var result = generate_event_times(group)
        // console.log(result)
        should.exist(result)
        result.should.be.instanceOf(Array)
        result.should.have.lengthOf(7)
        result.forEach(function(row){
            row.should.have.keys('ts','detectors')
            return null
        })
        result[0].ts.should.eql('2012-09-06 14:00:00')
        result[0].detectors.should.eql(['A'])

        result[1].ts.should.eql('2012-09-06 16:00:00')
        result[1].detectors.should.eql(['A','C'])

        result[2].ts.should.eql('2012-09-12 01:00:00')
        result[2].detectors.should.eql(['A'])

        result[3].ts.should.eql('2012-09-12 14:00:00')
        result[3].detectors.should.eql(['A','D'])

        result[4].ts.should.eql('2012-09-16 03:00:00')
        result[4].detectors.should.eql(['A','B','D'])

        result[5].ts.should.eql('2012-10-29 02:00:00')
        result[5].detectors.should.eql(['A','B','D','E'])

        result[6].ts.should.eql('2012-12-28 10:00:00')
        result[6].detectors.should.eql(['E'])


    })
})
