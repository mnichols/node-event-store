describe 'redis-streamer', ->
    es = require 'event-stream'
    {Redis} = require '../redis-streamer'
    describe 'simple', ->
        it 'should work', (done) ->
            sut = new Redis {db: 11}
            stream = sut.stream()
            ck = es.through (reply) ->
                reply.should.equal 'OK'
                done()
            stream.pipe(ck)
            stream.write 'flushdb'

    describe 'load', ->

        it 'should be ok', (done) ->
            process.env.NODE_DEBUG=true
            @timeout 0
            client = new Redis {db: 11}
            range = [0...1500]
            ticks = 0
            stream = client.stream()
            ck = es.through (reply) ->
                console.log 'ckreply', ticks
                if ++ticks==range.length
                    console.log 'ticked', ticks
                    done()
            pumper = es.readable (ct, cb) ->
                return pumper.emit 'end' if ct==range.length
                console.log 'ct', ct
                cb null, ['set', 'testload', range[ct]]
            stream.on 'error', (err) ->
                console.error 'ticks', ticks
                done err
            pumper.pipe(stream).pipe(ck)


    describe 'sortedsets', ->
        client = null
        stream = null
        setName = 'testsortedset'
        values = [ 
            'test-value-1'
            'test-value-2'
            'test-value-3' 
            'test-value-4' 
            'test-value-5' 
            'test-value-6' 
            'test-value-7' 
            'test-value-8' 
            'test-value-9' 
            'test-value-0' 
        ]
        beforeEach ->
            client = new Redis {db: 11}

        afterEach (done) ->
            stream.end()
            done()

        it 'zadd works', (done) ->
            tick = 0
            ck = es.through (reply) ->
                tick++
                Number(reply).should.equal 1
                done() if tick==values.length
            stream = client.stream('zadd', setName)
            stream.pipe(ck)
            values.forEach (val, i) ->
                stream.write [i, val]

        it 'zcard works', (done) ->
            ck = es.through (reply) ->
                Number(reply).should.equal values.length
                done()
            stream = client.stream('zcard')
            stream.pipe(ck)
            stream.write setName

        it 'zrangebyscore works', (done) ->
            expect= [
                'test-value-2'
                'test-value-3' 
                'test-value-4' 
            ]
            ck = es.through (reply) ->
                reply.should.equal expect.shift()+''
                done() if expect.length==0

            stream = client.stream('zrangebyscore', setName)
            stream.pipe(ck)
            stream.write [1,3]

        it 'zrevrangebyscore_with_scores works', (done) ->
            expect= [
                'test-value-4' 
                3
                'test-value-3' 
                2
                'test-value-2'
                1
            ]
            ck = es.through (reply) ->
                reply.should.equal expect.shift()+''
                done() if expect.length==0

            stream = client.stream('zrevrangebyscore', setName)
            stream.pipe(ck)
            stream.write [3, 1, 'WITHSCORES']

        it 'zrem works', (done) ->
            ticks = 0 
            ck = es.through (reply) ->
                Number(reply).should.equal 1
                done() if ++ticks==values.length

            stream = client.stream('zrem', setName)
            stream.pipe(ck)
            values.forEach (val) ->
                stream.write val
            
    describe 'strings', ->
        client = null
        stream = null

        beforeEach ->
            client = new Redis {db: 11}

        afterEach (done) ->
            stream.end()
            done()
        it 'set works', (done) ->
            ck = es.through (reply) ->
                reply.should.equal 'OK'
                done()
            stream = client.stream('set', 'testkey')
            stream.pipe(ck)
            stream.write 'testvalue'
        it 'strlen works', (done) ->
            ck = es.through (reply) ->
                Number(reply).should.equal 9
                done()
            stream = client.stream('strlen')
            stream.pipe(ck)
            stream.write 'testkey'
        it 'getset works', (done) ->
            ck = es.through (reply) ->
                reply.should.equal 'testvalue'
                done()
            stream = client.stream('getset', 'testkey')
            stream.pipe(ck)
            stream.write '50'
        it 'get works', (done) ->
            ck = es.through (reply) ->
                Number(reply).should.equal 50
                done()
            stream = client.stream('get')
            stream.pipe(ck)
            stream.write 'testkey'
        it 'incr works', (done) ->
            ck = es.through (reply) ->
                Number(reply).should.equal 51
                done()
            stream = client.stream('incr')
            stream.pipe(ck)
            stream.write 'testkey'
        it 'decr works', (done) ->
            ck = es.through (reply) ->
                Number(reply).should.equal 50
                done()
            stream = client.stream('decr')
            stream.pipe(ck)
            stream.write 'testkey'

        it 'append works', (done) ->
            ck = es.through (reply) ->
                Number(reply).should.equal 3
                done()
            stream = client.stream('append', 'testkey')
            stream.pipe(ck)
            stream.write '0'

        it 'getrange works', (done) ->
            ck = es.through (reply) ->
                Number(reply).should.equal 50
                done()
            stream = client.stream('getrange', 'testkey', 0)
            stream.pipe(ck)
            stream.write 1

        it 'setrange works', (done) ->
            ck = es.through (reply) ->
                Number(reply).should.equal 5
                done()
            stream = client.stream('setrange', 'testkey', 0)
            stream.pipe(ck)
            stream.write 50000





