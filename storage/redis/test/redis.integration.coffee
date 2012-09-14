describe 'redis-integration', ->
    cli = null
    cfg = null
    es = require '../../../event-store'
    Redis = require 'redis-stream'
    redis = require '../redis-stream-storage'
    redisAuditor = require '../redis-auditor'
    eventStream = require 'event-stream'

    beforeEach (done) ->
        (cli = new Redis 6379, 'localhost', 11)
        cfg =
            id: 'myhairystorage'
            client: cli
        done()

    afterEach (done) ->
        flusher = cli.stream()
        flusher.on 'data', (reply) -> 
            flusher.end()
            done()
        flusher.write 'flushdb'

    describe 'redis-auditor', ->
        describe '#throughput', ->
            ts = new Date().getTime()
            numberOfCommits = 100000
            beforeEach (done) ->
                @timeout(0)
                @auditor = redisAuditor.createAuditor cfg
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
                stream.on 'data', (data) ->
                    data.a.should.equal 1 if data.a
                    data.b.should.equal 2 if data.b
                    data.c.should.equal 3 if data.c
                    tick++
                stream.on 'end', =>
                    console.log '# events processed', tick
                    tick.should.equal (numberOfCommits*3)
                    done()

                stream.write()

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
                redisStorage = redis.createStorage(cfg)
                auditor = redisAuditor.createAuditor(cfg)
                storage = es(redisStorage, auditor)

                filter =
                    streamId: '123'
                    minRevision: 0
                    maxRevision: Number.MAX_VALUE

                stream = storage.open filter
                events = []
                aggregate = eventStream.map (data, next) ->
                    next null, data
                stream.on 'data', (data) ->
                    events.push data
                stream.on 'end', =>
                    aggregate.pipe(stream.commit).pipe eventStream.map (data, next) =>
                        expect = 
                            streamId : '123'
                            streamRevision:events.length + @commit1.streamRevision
                        checker = eventStream.pipeline(
                            cli.stream('zrange', auditor.auditKey, 0),
                            eventStream.parse(),
                            eventStream.map (data, next) ->
                                next null, data
                            eventStream.map (data, next) =>
                                data.streamId.should.equal expect.streamId
                                data.streamRevision.should.equal(expect.streamRevision)
                                next null, null
                                checker.end()
                            )
                        checker.on 'end', -> done()
                        checker.write '1'
                    aggregate.write events

                stream.read()
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
                storage = es(redis.createStorage(cfg))

                filter =
                    streamId: '123'
                    minRevision: 0
                    maxRevision: Number.MAX_VALUE

                stream = storage.open filter
                events = []
                aggregate = eventStream.map (data, next) ->
                    next null, data
                stream.on 'data', (data) ->
                    events.push data
                stream.on 'end', ->
                    aggregate.pipe(stream.commit).pipe eventStream.map (data, next) ->
                        done()
                    aggregate.write events

                stream.read()


        describe '#pipe into aggregate', ->
            beforeEach (done) ->
                @commit1 =
                    checkRevision: 0
                    headers: []
                    streamId: '123'
                    streamRevision: 3
                    payload: [
                        {a:1}
                        {b:2}
                        {c:3}
                    ]
                    timestamp: new Date(2012,9,1,12,0,0)
                seed = cli.stream('zadd', 'commits:123', 1)
                seed.on 'end', ->
                    done()
                seed.write JSON.stringify(@commit1)
                seed.end()

            it 'should work', (done) ->
                storage = es(redis.createStorage(cfg))
                filter =
                    streamId: '123'
                    minRevision: 0
                    maxRevision: Number.MAX_VALUE
                stream = storage.open filter
                received= []
                stream.on 'error', (err) ->
                    console.log 'error', err
                stream.on 'end', ->
                    received.length.should.equal 3
                    done()

                aggregate = eventStream.map (event, next) ->
                    #do stuff with event here
                    received.push event
                    next()
                stream.pipe aggregate
                stream.read()

