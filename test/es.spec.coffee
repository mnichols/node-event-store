es = require 'event-stream'
describe 'es', ->
    createCommit = (streamId, events=[]) ->
        streamId: streamId        
        timestamp: new Date().getTime()
        headers: []
        payload: events

    
    committable = (stream, storage) ->
        streamId = stream.streamId
        uncommitted = []
        commitStream = es.map (data, callback) ->
            storage.write data, callback

        addEvents: (events = []) ->
            uncommitted = uncommitted.concat events
            @

        commit: ->
            commitStream.write createCommit(streamId, uncommitted)

    eventStream = (storage) ->
        open: (filter) ->
            stream =
                es.map (filter, callback) ->
                    unless filter.streamId
                        return callback new Error 'streamId is required'

                    storage.read filter, (err, result) ->
                        return callback err if err
                        (filter[k]=result[k]) for k,val of result
                        stream.makeCommittable = -> committable stream, storage
                        callback null, filter
            (stream[k]=filter[k]) for k,val of filter
            stream.read = -> stream.write filter
            stream
                    
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
            stream.on 'data', (stream) ->
                stream.committedEvents.length.should.equal 1
                stream.committedEvents[0].should.eql
                    a:1
                done()
            stream.read()

    describe '#commit', ->
        it 'should write events to storage', ->
            event = { name: 'a'}
            inmem = ->
                events = {}
                write: (commit, cb) -> 
                    events[commit.streamId] = (events[commit.streamId] ? []).concat [commit]
                    cb null, commit
                events: events
            storage = inmem()
            sut = committable {streamId: '123'}, storage
            sut.addEvents [event]
            sut.commit()
            storage.events['123'].length.should.equal 1
            storage.events['123'][0].payload.should.eql [{name: 'a'}]
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
                writeable.addEvents events
                writeable.commit()
                storage.events['123'][0].payload.should.eq; [
                    {c:1}
                ]
            stream.read()



