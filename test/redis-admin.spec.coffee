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

    describe '#throughput', ->
        ts = new Date().getTime()
        numberOfCommits = 100000
        beforeEach (done) ->
            @timeout(0)
            sut = redisAdmin
            @admin = sut.createAdmin cfg
            @admin.on 'ready', (err, admin) =>
                streamId = 123
                writer = cli.stream('zadd')
                writer.on 'end', =>
                    streamId = 123
                    auditor = cli.stream('zadd')
                    auditor.on 'end', ->
                        done()
                    for c in [0...numberOfCommits]
                        auditor.write ["streamId2RevByTime", 
                            ts,
                            JSON.stringify { 
                                    streamId: (streamId++).toString()
                                    streamRevision: 1
                                }
                            ]
                    auditor.end()

                for c in [0...numberOfCommits]
                    commit = 
                        streamId: (streamId++).toString()
                        streamRevision: 1
                        payload: [{a:1},{b:2},{c:3}]

                    writer.write ["commits:#{commit.streamId.toString()}", 
                        1,
                        JSON.stringify(commit)
                    ]
                writer.end()
        it 'should not suck', (done) ->
            #we are pushing thru 300,000 events in about 20 seconds
            @timeout(20000)
            stream = @admin.createEventStream()
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
                    {d:1}
                    {e:2}
                    {f:3}
                ]
                timestamp: new Date(2012,9,8,23,0,0)
            @commit3 =
                headers: []
                streamId: '456'
                streamRevision: 1
                payload: [
                    {g:1}
                    {h:2}
                    {i:3}
                ]
                timestamp: new Date(2012,9,9,3,59,0)
            @commit4 =
                headers: []
                streamId: '789'
                streamRevision: 1
                payload: [
                    {j:1}
                    {k:2}
                    {l:3}
                ]
                timestamp: new Date(2012,9,9,6,59,0)
            @commit5 =
                headers: []
                streamId: '987'
                streamRevision: 1
                payload: [
                    {m:1}
                    {n:2}
                    {o:3}
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

        describe '#createRangeStream', ->
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
        describe '#createEventStream', ->
            it 'should create chronological event stream given datetime range', (done) ->
                start = new Date(2012, 9, 8, 7, 0, 0).getTime()
                end = new Date(2012, 9, 9, 7, 0, 0).getTime()
                stream = @admin.createEventStream()
                vals = []
                stream.on 'data', (data) ->
                    vals.push data
                stream.on 'end', =>
                    vals.length.should.equal 9
                    vals[0].should.eql @commit2.payload[0]
                    vals[1].should.eql @commit2.payload[1]
                    vals[2].should.eql @commit2.payload[2]
                    vals[3].should.eql @commit3.payload[0]
                    vals[4].should.eql @commit3.payload[1]
                    vals[5].should.eql @commit3.payload[2]
                    vals[6].should.eql @commit4.payload[0]
                    vals[7].should.eql @commit4.payload[1]
                    vals[8].should.eql @commit4.payload[2]
                    done()
                stream.write [start, end]


