es = require 'event-stream'
store = require '..'
inMem = require '../in-memory-storage'
describe 'event-store', ->
    storage = null
    Aggregate = ->
        events = []
        events: events
        in: es.map (data, next) ->
            events.push data
            next()
        out: es.map (data, next) ->
            next null, data
        
    beforeEach ->
        storage = inMem.createStorage()

    
    describe '#open to read', ->
        it 'should read events from storage', (done) ->
            storage.mount 
                'commits:123': [
                    streamRevision: 1
                    payload: [
                        {a:1}
                    ]
                ]
            sut = store storage
            filter = 
                minRevision: 0
                maxRevision: Number.MAX_VALUE
                streamId: '123'
            received = []
            stream = sut.open filter
            agg = new Aggregate()
            stream.on 'end', ->
                agg.events.length.should.equal 1
                agg.events[0].should.eql
                    a: 1
                done()
            stream.pipe agg.in
            stream.read()

    
    describe '#commit', ->
        beforeEach ->
            storage.mount 
                'commits:123': [
                    streamRevision: 1
                    payload: [
                        {a:1}
                    ]
                ]
        it 'should commit events', (done) ->
            sut = store storage
            filter = 
                minRevision: 0
                maxRevision: Number.MAX_VALUE
                streamId: '123'
            received = []
            stream = sut.open filter

            agg = Aggregate()
            stream.pipe(agg.in)
            stream.on 'end', =>
                ck = es.map (commit, next) ->
                    commit.should.exist
                    commit.streamRevision.should.equal 2
                    done()
                agg.out.pipe(stream.commit).pipe ck
                agg.out.write {b:1}
            stream.read()

