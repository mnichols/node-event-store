describe 'redis-admin', ->
    cli = null
    cfg = null
    Redis = require 'redis-stream'
    redisAdmin = require '../redis-admin'

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

    describe '#stream history', ->
        beforeEach (done) ->
            @commit1 =
                headers: []
                streamId: '123'
                streamRevision: 1
                payload: [
                    {a:1}
                    {b:2}
                    {c:3}
                ]
                timestamp: new Date(2012,9,1,12,0,0)
            @commit2 =
                headers: []
                streamId: '123'
                streamRevision: 4
                payload: [
                    {a:1}
                    {b:2}
                    {c:3}
                ]
                timestamp: new Date(2012,9,8,23,0,0)
            @commit3 =
                headers: []
                streamId: '456'
                streamRevision: 1
                payload: [
                    {a:1}
                    {b:2}
                    {c:3}
                ]
                timestamp: new Date(2012,9,9,3,59,0)
            @commit4 =
                headers: []
                streamId: '789'
                streamRevision: 1
                payload: [
                    {a:1}
                    {b:2}
                    {c:3}
                ]
                timestamp: new Date(2012,9,9,6,59,0)
            @commit5 =
                headers: []
                streamId: '987'
                streamRevision: 1
                payload: [
                    {a:1}
                    {b:2}
                    {c:3}
                ]
                timestamp: new Date(2012,9,10,12,0,0)
            @commits = [
                @commit1
                @commit2
                @commit3
                @commit4
                @commit5
            ]

            sut = redisAdmin
            @admin = sut.createAdmin cfg
            @admin.on 'ready', (err, admin) =>
                writer = cli.stream('zadd')
                writer.on 'end', =>
                    auditor = cli.stream('zadd')

                    auditor.on 'end', ->
                        done()
                    for c in @commits
                        auditor.write ["streamId2RevByTime", 
                            c.timestamp.getTime(), 
                            JSON.stringify { 
                                    streamId: c.streamId,
                                    streamRevision: c.streamRevision
                                }
                            ]
                    auditor.end()

                for c in @commits
                    writer.write ["commits:#{c.streamId}", 
                        c.streamRevision, 
                        JSON.stringify(c)]
                writer.end()
        

        it 'should emit streamid mapping between given datetime range', (done) ->
            start = new Date(2012, 9, 8, 7, 0, 0).getTime()
            end = new Date(2012, 9, 9, 7, 0, 0).getTime()
            stream = @admin.createRangeStream()
            vals = []
            stream.on 'data', (data) ->
                vals.push data
            stream.on 'end', ->
                vals.length.should.equal 3
                done()
            stream.write [start, end]
            stream.end()

        it 'should create chronological event stream given datetime range', (done) ->
            start = new Date(2012, 9, 8, 7, 0, 0).getTime()
            end = new Date(2012, 9, 9, 7, 0, 0).getTime()
            stream = @admin.createEventStream()
            vals = []
            stream.on 'data', (data) ->
                vals.push data
            stream.on 'end', ->
                vals.length.should.equal 9
                done()
            stream.write [start, end]
            stream.end()



