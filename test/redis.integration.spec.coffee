describe 'redis-integration', ->
    cli = null
    cfg = null
    es = require '../event-store'
    Redis = require 'redis-stream'
    redis = require '../storage/redis/redis-stream-storage'
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

    describe '#pipe from aggregate', ->
        beforeEach (done) ->
            @commit1 =
                checkRevision: 0
                headers: []
                streamId: '123'
                streamRevision: 1
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
            aggregate = eventStream.readable (ct, cb) ->
                cb null, [
                    {my: 'event'}
                ]
            stream.on 'end', ->
                aggregate.pipe(stream.commit).pipe eventStream.map (data, next) ->
                    done()

            stream.read()


    describe '#pipe into aggregate', ->
        beforeEach (done) ->
            @commit1 =
                checkRevision: 0
                headers: []
                streamId: '123'
                streamRevision: 1
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

