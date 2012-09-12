store = require '../event-store'
describe 'event-store', ->
    describe.skip '#openStream', ->
        it 'should read events from storage', (done) ->
            events = [
                {b:1}
            ]
            inmem = ->
                read: (q, cb) ->
                    cb null, 
                        streamRevision: 2
                        committedEvents: [{a:1}]
            sut = store inmem()
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

    
    describe.skip '#through', ->
        it 'should commit events', ->
            events = [
                {c:1}
            ]
            inmem = ->
                events = {}
                createReader: (filter) ->
                    read: (q, cb) ->
                        cb null, 
                            streamRevision: 2
                            committedEvents: [{a:1}]
                write: (commit, cb) ->
                    events[commit.streamId] = (events[commit.streamId] ? []).concat [commit]
                    cb null, commit
                events: events
            storage = inmem()
            sut = store storage
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



