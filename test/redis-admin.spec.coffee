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
        beforeEach ->
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


        it 'should emit events between given datetime range', (done) ->
            sut = redisAdmin
            redisAdmin.on 'ready', (err, admin) ->

                start = new DateTime(2012, 9, 8, 7, 0, 0).getTime()
                end = new DateTime(2012, 9, 9, 7, 0, 0).getTime()
                
                writer = cli.stream('zadd')
                writer.on 'end', ->
                    auditor = cli.stream('zadd')

                    auditor.on 'end', ->
                        range = admin.createRangeStream()
                        vals = []
                        range.on 'data', (data) ->
                            vals.push data
                        range.on 'end', ->
                            vals.length.should.equal 3
                            done()
                        range.write start, end
                    for c in @commits
                        auditor.write ["streamId2RevByTime", c.timestamp.getTime(), c.streamRevision]
                        auditor.end()

                for c in @commits
                    writer.write ["commits:#{c.streamId}", c.streamRevision, JSON.stringify(c)]
                    writer.end()
