committable = require '../committable'

describe 'committable', ->

    describe '#commit', ->
        it 'should write events to storage', (done) ->
            event = { name: 'a'}
            inmem = ->
                events = {}
                write: (commit, cb) -> 
                    events[commit.streamId] = (events[commit.streamId] ? []).concat [commit]
                    cb null, commit
                events: events
            storage = inmem()
            sut = committable {streamId: '123'}, storage
            sut.on 'end', ->
                storage.events['123'].length.should.equal 1
                storage.events['123'][0].payload.should.eql [{name: 'a'}]
                storage.events['123'][0].streamRevision.should.equal 1
                done()
            sut.commit [event]
