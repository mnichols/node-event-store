es = require 'event-stream'
describe 'commit-stream', ->
    CommitStream = require '../commit-stream'
    commit = ->
        streamId: '123'
        streamRevision: 2
        timestamp: Date.UTC 2012, 9,8,7,0,0,0
        checkRevision: 2
        headers: []

    it 'should buffer events received until end', (done) ->
        stream = new CommitStream commit()
        events = [
            {a:1}
            {b:2}
            {c:3}
        ]
        arra = es.readArray events
        stream.on 'data', (commit) ->
            commit.payload.length.should.equal 3
            commit.streamRevision.should.equal 5
        stream.on 'end', -> done()
        arra.pipe(stream)


    it 'should be readable', (done) ->
        stream = new CommitStream commit()
        events = [
            {a:1}
            {b:2}
            {c:3}
        ]
        arra = es.readArray events
        assertion = es.through (commit) ->
            commit.payload.length.should.equal 3
            commit.streamRevision.should.equal 5
            done()
        arra.pipe(stream).pipe(assertion)
