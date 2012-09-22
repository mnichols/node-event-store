es = require 'event-stream'
map = require 'map-stream'
committable = require '../committable'

describe 'committable', ->
    describe.skip 'given stream not committable', ->
        it 'should throw if source is not committable', (done) -> 
            notcommittable =  {streamId: 3, streamRevision: 0, committable: false}
            sut = committable notcommittable, 
                commitStream: -> 
                    es.through (fail) ->
                        done new Error 'should have failed'

            arr = es.readArray [
                {a:1}
                {b:2}
                {c:3}
            ]
            #this actually does as expected but catching errors in the stream isnt working
            arr.pipe(sut)
    describe 'given committable stream', ->
        describe '#piping events', ->
            
            it 'should write commit to underlying storage', (done) ->
                sut = committable {streamId: 3, streamRevision: 0, committable: true},
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

