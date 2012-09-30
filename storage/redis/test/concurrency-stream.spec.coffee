{Redis} = require '../redis-streamer'
es = require 'event-stream'
describe 'concurrency-stream', ->
    cli = null
    cfg = null
    beforeEach (done) ->
        cli = new Redis {db: 11}
        cfg =
            client: cli
            getCommitsKey: (id) -> "commits:#{id}"
        done()

    afterEach (done) ->
        flusher = cli.stream()
        flusher.on 'data', (reply) -> 
            flusher.end()
            done()
        flusher.write 'flushdb'
    describe '#pipe with current revision', ->
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
            stream.on 'end', -> done()
            stream.write JSON.stringify(@commit1)
            stream.end()
        it 'should pipe commit', (done) ->
            @timeout(100)
            sut = require '../concurrency-stream'
            stream = sut(cfg)
            input = es.readArray [@commit]
            pipedCommit = null
            input.on 'error', (err) ->
                done new Error 'should not have errored'
            input.pipe(stream).pipe es.through (commit) ->
                commit.streamRevision.should.equal 4
                done()

    describe '#pipe with old revision', ->
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
            sut = require '../concurrency-stream'
            stream = sut(cfg)
            input = es.readArray [@commit]
            stream.on 'error', (err) ->
                err.should.exist
                err.name.should.equal 'ConcurrencyError'
                err.message.should.equal 'Expected 0, but got 1'
                done()
            input.pipe stream
