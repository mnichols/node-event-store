describe 'redis-streamer', ->
    es = require 'event-stream'
    {Redis} = require '../redis-streamer'
    Duplex = require '../node_modules/readable-stream/duplex'
    Transform = require '../node_modules/readable-stream/transform'
    Writable = require '../node_modules/readable-stream/writable'
    Readable = require 'readable-stream'

    describe 'simple', ->
        it 'should work', (done) ->
            sut = new Redis {db: 11}
            stream = sut.stream()
            ck = es.through (reply) ->
                reply.should.equal 'OK'
                done()
            stream.pipe(ck)
            stream.write 'flushdb'

    describe.skip 'load many streams', ->

        it 'should be ok', (done) ->
            @timeout 20000
            client = new Redis {db: 11}
            range = [0...2000]
            ticks = 1
            for i in range
                stream = client.stream()
                ck = es.through (reply) ->
                    ticks++
                stream.on 'done', ->
                    done() if ticks==range.length
                stream.on 'error', (err) ->
                    console.log err
                    done err
                stream.pipe(ck)
                stream.write  ['set', 'testload', range[i]]
    describe 'load single stream', ->
        it 'should be ok', (done) ->
            @timeout 0
            client = new Redis {db: 11}
            class Pump extends Readable
                constructor: (@arr=[])->
                    @cmdFn = client.formatCommand()
                    all = @arr.map (i) => @cmdFn i
                    @buffer = new Buffer all
                    @start = 0
                    super
                _read: (n, cb) ->
                    if @start >= @buffer.length
                        return cb()
                    end = if n > @buffer.length then @buffer.length else n
                    data = new Buffer(end)
                    @buffer.copy data, 0, @start, end
                    @start+=n
                    cb null, data

            class Ck extends Writable
                constructor: ->
                    super
                _write: (chunk, cb) ->
                    reply = chunk.toString()

            pumper = new Pump [0...20].map (r) -> [
                'set'
                'testload'
                r+''
            ]
            stream = client.stream()
            stream.on 'finish', ->
                pumper = new Pump [['get', 'testload']]
                stream = client.stream()
                ck = new Ck()
                #ck.on 'finish', -> done()
                pumper.pipe(stream).pipe(ck)



            pumper.pipe(stream)


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
                done() if tick==values.length-1
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
                done() if ++ticks==values.length-1

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





