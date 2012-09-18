es = require 'event-stream'
map = require 'map-stream'
committable = require '../committable'

describe 'committable', ->
    describe '#piping events', ->
        it 'should write commit to underlying storage', (done) ->
            sut = committable {streamId: 3},
                commitStream: -> es.through (commit) ->
                    commit.should.exist
                    commit.streamRevision.should.equal 3
                    done()

            arr = es.readArray [
                {a:1}
                {b:2}
                {c:3}
            ]
            arr.pipe sut

                

    describe.skip '#commit', ->
        it 'should write events to storage', (done) ->
            event = { name: 'a'}
            events = {}
            sut = committable {streamId: '123'}, 
                createCommitter: ->
                    map (commit, next) ->
                        events[commit.streamId] = (events[commit.streamId] ? []).concat [commit]
                        next null, commit
                        sut.end()
            sut.on 'end', ->
                events['123'].length.should.equal 1
                events['123'][0].payload.should.eql [{name: 'a'}]
                events['123'][0].streamRevision.should.equal 1
                done()
            sut.write [event]
