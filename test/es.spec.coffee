eventStream = require '../event-stream'
committable = require '../committable'
describe 'es', ->
    describe '#openStream', ->
        it 'should read events from storage', (done) ->
            events = [
                {b:1}
            ]
            inmem = ->
                read: (q, cb) ->
                    cb null, 
                        streamRevision: 2
                        committedEvents: [{a:1}]
            sut = eventStream inmem()
            filter = 
                minRevision: 0
                maxRevision: Number.MAX_VALUE
                streamId: '123'
            stream = sut.open filter
            stream.on 'end', done
            stream.on 'data', (read) ->
                read.committedEvents.length.should.equal 1
                read.committedEvents[0].should.eql
                    a:1
            stream.read()

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
    
    describe '#through', ->
        it 'should commit events', ->
            events = [
                {c:1}
            ]
            inmem = ->
                events = {}
                read: (q, cb) ->
                    cb null, 
                        streamRevision: 2
                        committedEvents: [{a:1}]
                write: (commit, cb) ->
                    events[commit.streamId] = (events[commit.streamId] ? []).concat [commit]
                    cb null, commit
                events: events
            storage = inmem()
            sut = eventStream storage
            filter = 
                streamId: '123'
                minRevision: 0
                maxRevision: Number.MAX_VALUE
            stream = sut.open filter
            stream.on 'data', (e) ->
                e.committedEvents.should.eql [{a:1}]
                
                writeable = stream.makeCommittable()
                writeable.commit events
                storage.events['123'][0].payload.should.eq; [
                    {c:1}
                ]
            stream.read()



