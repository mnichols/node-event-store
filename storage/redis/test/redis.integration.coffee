describe 'redis-integration', ->
    cli = null
    cfg = null
    eventStore = require '../../../event-store'
    redis = require '..'
    es = require 'event-stream'

    beforeEach (done) ->
        cli = redis.createClient({db:11})
        eventStorage = redis.createStorage
            client: redis.createClient({db:11})
        cfg =
            client: redis.createClient({db:11})
            eventStorage: eventStorage
        done()

    afterEach (done) ->
        flusher = cli.stream()
        flusher.on 'end', ->
            done()
        flusher.write 'flushdb'
        flusher.end()

    createAggregate = ->
        Aggregate = ->
        agg = new Aggregate()
        bucket = require('../../../bucket-stream')()
        es.pipeline bucket, es.map (events, next) ->
            agg.events = events
            next null, agg
    describe 'redis-auditor', ->
        describe '#throughput', ->
            ts = new Date().getTime()
            numberOfCommits = 1500
            beforeEach (done) ->
                @timeout(0)
                @auditor = redis.createAuditor cfg
                @auditor.on 'ready', (err) =>
                    streamId = 123
                    writer = cli.stream('zadd')
                    writer.on 'end', =>
                        streamId = 123
                        streamer = cli.stream('zadd')
                        streamer.on 'end', ->
                            done()
                        for c in [0...numberOfCommits]
                            streamer.write [@auditor.auditKey, 
                                ts,
                                JSON.stringify { 
                                        streamId: (streamId++).toString()
                                        streamRevision: 1
                                    }
                                ]
                        streamer.end()

                    for c in [0...numberOfCommits]
                        commit = 
                            streamId: (streamId++).toString()
                            streamRevision: 3
                            payload: [{a:1},{b:2},{c:3}]

                        writer.write ["commits:#{commit.streamId.toString()}", 
                            1,
                            JSON.stringify(commit)
                        ]
                    writer.end()
            it 'should not suck', (done) ->
                #we are pushing thru 300,000 events in about 20 seconds
                @timeout(20000)
                stream = @auditor.createEventStream()
                tick = 0
                stream.on 'error', (err) -> done err
                stream.on 'data', (data) ->
                    data.a.should.equal 1 if data.a
                    data.b.should.equal 2 if data.b
                    data.c.should.equal 3 if data.c
                    tick++
                stream.on 'end', =>
                    console.log '# events processed', tick
                    tick.should.equal (numberOfCommits*3)
                    done()

                stream.read()

    describe 'event-store', ->
        describe '#auditable-pipe from aggregate', ->
            beforeEach (done) ->
                @commit1 =
                    checkRevision: 3
                    headers: []
                    streamId: '123'
                    streamRevision: 3
                    payload: [
                        {a:1}
                        {b:2}
                        {c:3}
                    ]
                    timestamp: new Date(2012,9,1,12,0,0)
                seed = cli.stream('zadd', 'commits:123', 3)
                seed.on 'end', ->
                    done()
                seed.write JSON.stringify(@commit1)
                seed.end()

            it 'should work', (done) ->
                @timeout 100
                redisStorage = redis.createStorage(cfg)
                auditor = redis.createAuditor(cfg)
                storage = eventStore(redisStorage, auditor)

                filter =
                    streamId: '123'
                    minRevision: 0
                    maxRevision: Number.MAX_VALUE

                stream = storage.open filter
                aggregate = createAggregate()
                assertion = es.through (commit) =>
                    expect = 
                        streamId : '123'
                        streamRevision:1 + @commit1.streamRevision

                    verify = es.through (data) =>
                        data.streamId.should.equal expect.streamId
                        data.streamRevision.should.equal(expect.streamRevision)
                        done()
                    checker = cli.stream()
                    checker.pipe(es.parse()).pipe(verify)
                    checker.write ['zrange', auditor.auditKey, 0, 1 ]

                stream.on 'end', ->
                    pending = es.readArray [{d:4}]
                    pending.pipe(stream.commit()).pipe(assertion)

                stream.pipe(aggregate)
        describe '#pipe from aggregate', ->
            beforeEach (done) ->
                @commit1 =
                    checkRevision: 3
                    headers: []
                    streamId: '123'
                    streamRevision: 3
                    payload: [
                        {a:1}
                        {b:2}
                        {c:3}
                    ]
                    timestamp: new Date(2012,9,1,12,0,0)
                seed = cli.stream('zadd', 'commits:123', 3)
                seed.on 'end', ->
                    done()
                seed.write JSON.stringify(@commit1)
                seed.end()

            it 'should work', (done) ->
                storage = eventStore(redis.createStorage(cfg))

                filter =
                    streamId: '123'
                    minRevision: 0
                    maxRevision: Number.MAX_VALUE

                
                stream = storage.open filter
                aggregate = createAggregate()
                stream.on 'end', ->
                    pending = es.readArray [{d:4}]
                    assertion = es.through (commit) ->
                        done()
                    pending.pipe(stream.commit()).pipe(assertion)
                stream.pipe(aggregate)


        describe '#pipe into aggregate', ->
            streamId = 'intoagg'
            beforeEach (done) ->
                @commit1 =
                    checkRevision: 0
                    headers: []
                    streamId: streamId
                    streamRevision: 3
                    payload: [
                        {a:1}
                        {b:2}
                        {c:3}
                    ]
                    timestamp: new Date(2012,9,1,12,0,0)
                seed = cli.stream('zadd', 'commits:intoagg', 1)
                seed.on 'end', ->
                    done()
                seed.write JSON.stringify(@commit1)
                seed.end()

            it 'should work', (done) ->
                @timeout 10
                storage = eventStore(redis.createStorage(cfg))
                filter =
                    streamId: streamId
                    minRevision: 0
                    maxRevision: Number.MAX_VALUE
                stream = storage.open filter
                received= []
                stream.on 'error', (err) ->
                    console.log 'error', err
                stream.on 'end', ->
                    received.length.should.equal 3
                    done()

                aggregate = es.through (event) ->
                    #do stuff with event here
                    received.push event
                stream.pipe aggregate

