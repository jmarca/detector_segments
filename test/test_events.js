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
                'mints':'2012-09-06 14:00:00',
                'maxts':'2012-12-28 09:00:00'
            },
            {
                'id':'B',
                'mints':'2012-09-16 03:00:00',
                'maxts':'2012-12-28 09:00:00'
            },
            {
                'id':'C',
                'mints':'2012-09-06 16:00:00',
                'maxts':'2012-09-12 00:00:00'
            },
            {
                'id':'D',
                'mints':'2012-09-12 14:00:00',
                'maxts':'2012-12-28 09:00:00'
            },
            {
                'id':'E',
                'mints':'2012-10-29 02:00:00',
                'maxts':'2012-12-28 18:00:00'
            }
        ]
        var result = generate_event_times(group)
        console.log(result)
        should.exist(result)
        result.should.be.instanceOf(Array)
        result.should.have.lengthOf(7)

    })
})
