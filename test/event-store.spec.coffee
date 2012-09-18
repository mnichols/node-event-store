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


    
    describe.skip '#commit', ->
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
                commit = stream.commit

                ck = es.map (commit, next) ->
                    commit.should.exist
                    commit.streamRevision.should.equal 2
                    done()
                agg.out.pipe(stream.commit).pipe ck
                agg.out.write {b:1}
            stream.read()

