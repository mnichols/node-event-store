describe 'redis-stream storage', ->
    cli = null
    Redis = require 'redis-stream'
    es = require 'event-stream'
    beforeEach (done) ->
        cli = new Redis 6379, 'localhost', 11
        done()

    afterEach (done) ->
        flusher = cli.stream()
        flusher.on 'data', (reply) -> 
            flusher.end()
            done()
        flusher.write 'flushdb'


    redisStorage = 
        createStorage: (client, cb) ->
            ConcurrencyError = ->
                Error.apply @, arguments
                @name = 'ConcurrencyError'

            ConcurrencyError:: = new Error()
            ConcurrencyError::constructor = ConcurrencyError
            isNumber = (obj) ->
                toString.call(obj)=='[object Number]'

            cb null,
                getEventsKey: (streamId) -> "events:#{streamId}"
                read: (filter, callback) ->
                    id = @getEventsKey(filter.streamId)
                    reader = client.stream()
                    err = null
                    events = []
                    push = (moreEvents=[]) -> Array::push.apply events, moreEvents
                    reader.on 'end', -> 
                        callback err, events
                    reader.on 'data', (data) ->
                        obj = JSON.parse data
                        push obj.payload

                    reader.on 'error', (error) -> 
                        console.error error
                        err = error
                    reader.redis.write Redis.parse [
                        'zrangebyscore'
                        id
                        filter.minRevision
                        filter.maxRevision ? -1
                    ]
                    reader.end()
                writeStream: (commit, callback) ->
                    id = @getEventsKey(commit.streamId)
                    reply = null
                    concurrency = client.stream 'zrevrange', id, 0, 1
                    check = (data, next) ->
                        return next data if ~data.indexOf '-ERR'
                        score = Number(data)
                        #first record is likely the actual object
                        return next() if isNaN score 
                        if score == commit.checkRevision
                            return next null, JSON.stringify(commit)
                        next new ConcurrencyError()
                    writer = client.stream('zadd', id, commit.streamRevision)
                    respond = (data, next) ->
                        reply = JSON.parse data
                        next null, reply

                    pipe = es.pipe(concurrency, es.map(check), writer, es.map(respond))

                    pipe.on 'error', callback
                    pipe.on 'end', -> callback null, reply
                    pipe.write 'WITHSCORES'
                    pipe.end()
                write: (commit, callback) ->
                    id = @getEventsKey(commit.streamId)
                    concurrency = client.stream()
                    check = (data, next) ->
                        score = Number(data)
                        #first record is likely the actual object
                        return if isNaN score 
                        return next null, null if score == commit.checkRevision
                        next new ConcurrencyError()

                    
                    writeCommit = (data, next) ->
                        reply = null
                        writer = client.stream 'zadd', id, commit.streamRevision
                        writer.on 'data', (data) ->
                            reply = JSON.parse data
                        writer.on 'end', ->
                            callback null, reply
                        #dont need to persist our check
                        delete commit.checkRevision
                        writer.write JSON.stringify commit
                        writer.end()

                    finish = writeCommit

                    concurrency.on 'data', (reply) ->
                        score = Number(reply)
                        #first record is likely the actual object
                        return if isNaN score 
                        return if score == commit.checkRevision
                        finish = -> callback new ConcurrencyError()

                    concurrency.on 'end', ->
                        finish()

                    concurrency.redis.write Redis.parse [
                        'zrevrange'
                        id
                        0
                        1
                        'WITHSCORES'
                    ]
                    concurrency.end()


    describe '#read-stream', ->
        it 'should read all events', (done) ->
            sut = redisStorage
            commit1 =
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
            seed = cli.stream('zadd', 'events:123', 1)
            seed.on 'end', ->
                sut.createStorage cli, (err, storage) ->
                    filter = 
                        streamId: '123'
                        minRevision: 0
                        maxRevision: Number.MAX_VALUE
                    storage.read filter, (err, result) ->
                        result.length.should.equal 3
                        done()
            seed.write JSON.stringify(commit1)
            seed.end()

    describe '#write-stream', ->
        it 'should write commit ok', (done) ->
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

            sut.createStorage cli, (err, storage) ->
                storage.write commit, (err, result) ->
                    args = ['events:123', 0, 4]
                    actual = cli.stream 'zrange', 'events:123', 0
                    data = null
                    actual.on 'error', (err) ->
                        console.log 'err', err
                    actual.on 'end', ->
                        data.should.exist
                        done()
                    actual.on 'data', (reply) ->
                        data = reply
                    actual.write -1
                    actual.end()
    describe '#write-stream with old revision', ->
        it 'should return concurrency error', (done) ->
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
            stream = cli.stream 'zadd', 'events:123', 1
            stream.on 'end', ->
                sut.createStorage cli, (err, storage) ->
                    storage.write commit, (err, result) ->
                        err.should.exist
                        err.name.should.equal 'ConcurrencyError'
                        done()
            stream.write JSON.stringify(commit1)
            stream.end()
    describe '#write-stream again', ->
        it 'should write commit ok', (done) ->
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
            stream = cli.stream 'zadd', 'events:123', 1
            stream.on 'end', ->
                sut.createStorage cli, (err, storage) ->
                    storage.write commit, (err, result) ->
                        actual = cli.stream 'zrangebyscore', 'events:123', 0
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


