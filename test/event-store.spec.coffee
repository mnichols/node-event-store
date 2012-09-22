{Stream} = require 'stream'
es = require 'event-stream'
store = require '..'
inMem = require '../in-memory-storage'
describe 'event-store', ->
    storage = null
        
    beforeEach ->
        storage = inMem.createStorage()

    createAggregate = ->
        Aggregate = ->
        agg = new Aggregate()
        bucket = require('../bucket-stream')()
        es.pipeline bucket, es.map (events, next) ->
            agg.events = events
            next null, agg
    
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
            aggStream = createAggregate()
            stream.pipe(aggStream).pipe es.through (agg) ->
                agg.events.length.should.equal 1
                agg.events[0].should.eql
                    a: 1
                done()


    
    describe '#commit', ->
        beforeEach ->
            storage.mount 
                'commits:123': [
                    streamRevision: 1
                    payload: [
                        {a:1}
                    ]
                ]
        it 'should support headers', (done) ->
            sut = store storage
            filter = 
                minRevision: 0
                maxRevision: Number.MAX_VALUE
                streamId: '123'
            received = []
            stream = sut.open filter


            aggStream = createAggregate()
            pending = es.map (agg, next) ->
                agg.events.should.eql [{a:1}]
                next null, {d:4}
            headers = [
                {correlationId: '1234'}
                {userName: 'me'}
            ]
            stream.pipe(aggStream)
                .pipe(pending)
                .pipe(stream.commit({headers: headers}))
                .pipe es.through (commit) ->
                    commit.headers.should.eql headers
                    done()
        it 'should pipe committed events', (done) ->
            sut = store storage
            filter = 
                minRevision: 0
                maxRevision: Number.MAX_VALUE
                streamId: '123'
            received = []
            stream = sut.open filter


            aggStream = createAggregate()
            pending = es.map (agg, next) ->
                agg.events.should.eql [{a:1}]
                next null, {d:4}
            stream.pipe(aggStream)
                .pipe(pending)
                .pipe(stream.commit())
                .pipe es.through (commit) ->
                    commit.streamRevision.should.equal 2
                    commit.payload.should.eql [ {d:4} ]
                    done()

