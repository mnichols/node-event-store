redisStorage = require '../redis-stream-storage'
cli = null
cfg = null
Redis = require 'redis-stream'
es = require 'event-stream'
describe 'redis-stream storage', ->
    beforeEach (done) ->
        (cli = new Redis 6379, 'localhost', 11)
        cfg =
            id: 'my-app-storage'
            client: cli
        done()

    afterEach (done) ->
        flusher = cli.stream()
        flusher.write 'flushdb'
        flusher.end()
        done()

    describe '#init', ->
        it 'should emit ready event', (done) ->
            sut = redisStorage
            storage = sut.createStorage cfg
            storage.on 'storage.ready', (storage) -> 
                storage.id.should.equal 'my-app-storage'
                done()



    describe '#createReader stream', ->
        sut = null
        describe 'given no commits', ->
            it 'should emit end', (done) ->
                @timeout(100)
                sut = redisStorage
                ck = es.through (event) ->
                    tick++
                    done new Error 'no data should have been passed'
                sut.createStorage cfg, (err, storage) =>
                    filter = 
                        streamId: '123'
                        minRevision: 0       
                        maxRevision: Number.MAX_VALUE
                    reader= storage.createReader filter
                    tick=0
                    reader.on 'end', =>
                        tick.should.equal 0
                        reader.streamRevision.should.equal 0
                        done()
                    reader.pipe(ck)
        describe 'given at least one commit', ->
        
            beforeEach (done) ->
                @timeout(100)
                sut = redisStorage
                @commit1 =
                    headers: []
                    streamId: '123'
                    streamRevision: 3
                    payload: [
                        {a:1}
                        {b:2}
                        {c:3}
                    ]
                    timestamp: new Date(2012,9,1,12,0,0)
                seed = cli.stream('zadd', 'commits:123', @commit1.streamRevision)
                seed.on 'end', ->
                    done()
                seed.write JSON.stringify(@commit1)
                seed.end()
            it 'should read all events', (done) ->
                @timeout(100)
                sut = redisStorage
                sut.createStorage cfg, (err, storage) =>
                    filter = 
                        streamId: '123'
                        minRevision: 0       
                        maxRevision: Number.MAX_VALUE
                    reader= storage.createReader filter, 
                        enrich: true
                    replies = []
                    tick=0
                    ck = es.through (data) ->
                        tick++
                        replies.push data
                    reader.on 'end', =>
                        tick.should.equal 3
                        reader.streamRevision.should.equal 3
                        replies.length.should.equal @commit1.payload.length
                        replies[0].streamId.should.equal '123'
                        replies[0].streamRevision.should.equal 3
                        replies[0].a.should.equal 1
                        replies[1].streamId.should.equal '123'
                        replies[1].streamRevision.should.equal 3
                        replies[1].b.should.equal 2
                        replies[2].streamId.should.equal '123'
                        replies[2].streamRevision.should.equal 3
                        replies[2].c.should.equal 3
                        done()
                    reader.pipe(ck)
    describe '#read', ->
        sut = null
        beforeEach (done) ->
            @timeout(100)
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
            seed = cli.stream('zadd', 'commits:123', 3)
            seed.on 'end', ->
                done()
            seed.write JSON.stringify(@commit1)
            seed.end()
        it 'should read all events', (done) ->
            sut = redisStorage
            sut.createStorage cfg, (err, storage) ->
                filter = 
                    streamId: '123'
                    minRevision: 0
                    maxRevision: Number.MAX_VALUE
                storage.read filter, (err, result) ->
                    result.length.should.equal 3
                    done()

    describe '#write-emit with old revision', ->
        beforeEach (done) ->
            @timeout(1000)
            @commit1 =
                checkRevision: 0
                headers: []
                streamId: '123'
                streamRevision: 1
                payload: [
                    {a:1}
                ]
                timestamp: new Date(2012,9,1,12,0,0)
            @commit =
                checkRevision: @commit1.checkRevision
                headers: []
                streamId: '123'
                streamRevision: 4
                payload: [
                    {a:1}
                    {b:2}
                    {c:3}
                ]
                timestamp: new Date(2012,9,1,13,0,0)
            stream = cli.stream 'zadd', 'commits:123', 1
            stream.on 'end', -> done()
            stream.write JSON.stringify(@commit1)
            stream.end()
        it 'should return concurrency error', (done) ->
            @timeout(100)
            sut = redisStorage
            sut = redisStorage
            sut.createStorage cfg, (err, storage) =>
                emitter = storage.commitStream @commit, cfg
                emitter.on 'commit', (data) =>
                    done new Error('fail')
                emitter.on 'error', (err) =>
                    err.should.exist
                    err.name.should.equal 'ConcurrencyError'
                    done()
                emitter.write @commit
    describe '#write-emit', ->
        it 'should publish storage commit event', (done) ->
            @timeout(100)
            sut = redisStorage
            commit =
                checkRevision: 0
                headers: []
                streamId: '123'
                streamRevision: 3
                payload: [
                    {a:1}
                    {b:2}
                    {c:3}
                ]
                timestamp: new Date(2012,9,1,13,0,0)

            sut.createStorage cfg, (err, storage) =>
                emitter =  storage.commitStream commit, cfg
                storage.on "#{cfg.id}.commit", -> done()
                emitter.write commit

        it 'should pipe commit ok', (done) ->
            @timeout(100)
            sut = redisStorage
            commit =
                checkRevision: 0
                headers: []
                streamId: '123'
                streamRevision: 3
                payload: [
                    {a:1}
                    {b:2}
                    {c:3}
                ]
                timestamp: new Date(2012,9,1,13,0,0)

            sut.createStorage cfg, (err, storage) =>
                emitter =  storage.commitStream commit, cfg
                meh = es.readable (cnt, callback) ->
                    if cnt > 0
                        return meh.emit 'end'
                    callback null, commit
                verify = es.through (data) ->
                    data.payload.should.eql commit.payload
                    done()
                meh.pipe(emitter).pipe(verify)
        it 'should write commit ok', (done) ->
            @timeout(100)
            sut = redisStorage
            commit =
                checkRevision: 0
                headers: []
                streamId: '123'
                streamRevision: 3
                payload: [
                    {a:1}
                    {b:2}
                    {c:3}
                ]
                timestamp: new Date(2012,9,1,13,0,0)

            sut.createStorage cfg, (err, storage) =>
                emitter =  storage.commitStream commit, cfg
                emitter.on 'commit', (data)=>
                    actual = cli.stream 'zrange', 'commits:123', 0
                    actual.on 'data', (reply) =>
                        data = JSON.parse(reply)
                        data.should.exist
                        data.payload.should.eql commit.payload
                        assert.isUndefined(data.checkRevision)
                        done()
                    actual.write -1

                emitter.write commit


    describe '#write-emit again', ->
        it 'should write commit ok', (done) ->
            @timeout(100)
            sut = redisStorage
            commit1 =
                checkRevision: 0
                headers: []
                streamId: '123'
                streamRevision: 1
                payload: [
                    {a:1}
                ]
                timestamp: new Date(2012,9,1,12,0,0)
            commit =
                checkRevision: 1
                headers: []
                streamId: '123'
                streamRevision: 4
                payload: [
                    {a:1}
                    {b:2}
                    {c:3}
                ]
                timestamp: new Date(2012,9,1,13,0,0)
            stream = cli.stream 'zadd', 'commits:123', 1
            stream.on 'end', ->
                sut.createStorage cfg, (err, storage) ->
                    emitter = storage.commitStream()
                    emitter.on 'error', (err) ->
                        done new Error 'fail'
                    emitter.on 'commit', (data) ->
                        actual = cli.stream 'zrangebyscore', 'commits:123', 0
                        replies = []
                        actual.on 'end', ->
                            replies.length.should.equal 2
                            replies[0].payload.should.eql commit1.payload
                            replies[1].payload.should.eql commit.payload
                            done()
                        actual.on 'data', (reply) ->
                            replies.push JSON.parse reply
                        actual.write 100
                        actual.end()
                    emitter.write commit
            stream.write JSON.stringify(commit1)
            stream.end()
    describe '#write-stream', ->
        it 'should write commit ok', (done) ->
            @timeout(100)
            sut = redisStorage
            commit =
                checkRevision: 0
                headers: []
                streamId: '123'
                streamRevision: 3
                payload: [
                    {a:1}
                    {b:2}
                    {c:3}
                ]
                timestamp: new Date(2012,9,1,13,0,0)

            sut.createStorage cfg, (err, storage) ->
                storage.write commit, (err, result) ->
                    args = ['commits:123', 0, 4]
                    actual = cli.stream 'zrange', 'commits:123', 0
                    data = null
                    actual.on 'error', (err) ->
                        console.error 'err', err
                        done(new Error 'fail')
                    actual.on 'end', ->
                        data.should.exist
                        done()
                    actual.on 'data', (reply) ->
                        data = reply
                    actual.write -1
                    actual.end()
    describe '#write-stream with old revision', ->
        it 'should return concurrency error', (done) ->
            @timeout(100)
            sut = redisStorage
            commit1 =
                checkRevision: 0
                headers: []
                streamId: '123'
                streamRevision: 1
                payload: [
                    {a:1}
                ]
                timestamp: new Date(2012,9,1,12,0,0)
            commit =
                checkRevision: commit1.checkRevision
                headers: []
                streamId: '123'
                streamRevision: 4
                payload: [
                    {a:1}
                    {b:2}
                    {c:3}
                ]
                timestamp: new Date(2012,9,1,13,0,0)
            stream = cli.stream 'zadd', 'commits:123', 1
            stream.on 'end', ->
                sut.createStorage cfg, (err, storage) ->
                    storage.write commit, (err, result) ->
                        err.should.exist
                        err.name.should.equal 'ConcurrencyError'
                        done()
            stream.write JSON.stringify(commit1)
            stream.end()
    describe '#write-stream again', ->
        it 'should write commit ok', (done) ->
            @timeout(100)
            sut = redisStorage
            commit1 =
                checkRevision: 0
                headers: []
                streamId: '123'
                streamRevision: 1
                payload: [
                    {a:1}
                ]
                timestamp: new Date(2012,9,1,12,0,0)
            commit =
                checkRevision: 1
                headers: []
                streamId: '123'
                streamRevision: 4
                payload: [
                    {a:1}
                    {b:2}
                    {c:3}
                ]
                timestamp: new Date(2012,9,1,13,0,0)
            stream = cli.stream 'zadd', 'commits:123', 1
            stream.on 'end', ->
                sut.createStorage cfg, (err, storage) ->
                    storage.write commit, (err, result) ->
                        actual = cli.stream 'zrangebyscore', 'commits:123', 0
                        replies = []
                        actual.on 'end', ->
                            replies.length.should.equal 2
                            replies[0].payload.should.eql commit1.payload
                            replies[1].payload.should.eql commit.payload
                            done()
                        actual.on 'data', (reply) ->
                            replies.push JSON.parse reply
                        actual.write 100
                        actual.end()
            stream.write JSON.stringify(commit1)
            stream.end()


