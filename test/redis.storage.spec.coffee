describe 'redis storage', ->
    cli = null
    beforeEach (done) ->
        redis = require 'redis'
        cli = redis.createClient 6379, 'localhost' 
        cli.on 'ready', ->
            cli.select 11, (err, ok) ->
                done()

    afterEach (done) ->
        cli.flushdb done

    redisStorage = 
        createStorage: (client, cb) ->
            ConcurrencyError = ->
                Error.apply @, arguments
                @name = 'ConcurrencyError'

            ConcurrencyError:: = new Error()
            ConcurrencyError::constructor = ConcurrencyError


            cb null, 
                read: (filter, callback) ->
                    args = [
                        "events:#{filter.streamId}"
                        filter.minRevision
                        filter.maxRevision
                    ]
                    client.zrangebyscore args, (err, reply) ->
                        events = []
                        reply.forEach (str) ->
                            commit = JSON.parse str
                            events = events.concat commit.payload
                        callback err, events
                write: (commit, callback) ->
                    id = "events:#{commit.streamId}"
                    args = [
                        id
                        0
                        1
                        'WITHSCORES'
                    ]
                    client.zrevrange args,  (err, reply) ->
                        return callback err if err
                        if Number(reply[1]) != commit.checkRevision
                            return callback new ConcurrencyError()
                        args = [
                            id
                            commit.streamRevision
                            JSON.stringify commit
                        ]
                        #@todo:check concurrency conflict
                        client.zadd args, (err, reply) ->
                            return callback err if err
                            callback err, reply
    describe '#read', ->
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

            cli.zadd ['events:123',1,JSON.stringify(commit1)], (err, reply) ->
                sut.createStorage cli, (err, storage) ->
                    filter = 
                        streamId: '123'
                        minRevision: 0
                        maxRevision: Number.MAX_VALUE
                    storage.read filter, (err, result) ->
                        result.length.should.equal 3
                        done()


    describe '#write', ->
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
                    args = ['events:123', 0, 4, 'WITHSCORES']
                    cli.zrangebyscore args, (err, reply) ->
                        
                        done()
    describe '#write stream with old revision', ->
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

            cli.zadd ['events:123',1,JSON.stringify(commit1)], (err, reply) ->
                cli.zrange 'events:123', 0, 1, (err, reply) ->
                    sut.createStorage cli, (err, storage) ->
                        storage.write commit, (err, result) ->
                            err.should.exist
                            err.name.should.equal 'ConcurrencyError'
                            done()
    describe '#write again', ->
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

            cli.zadd ['events:123',1,JSON.stringify(commit1)], (err, reply) ->
                sut.createStorage cli, (err, storage) ->
                    storage.write commit, (err, result) ->
                        args = ['events:123', 0, 4, 'WITHSCORES']
                        cli.zrangebyscore args, (err, reply) ->
                            reply.length.should.equal 4
                            reply[1].should.equal '1'
                            reply[3].should.equal '4'
                            done()


