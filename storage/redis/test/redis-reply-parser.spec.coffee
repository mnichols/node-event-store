describe 'redis-reply-parser', ->
    es = require 'event-stream'
    parser = require '../redis-reply-parser'
    describe 'single-line reply', ->
        beforeEach ->
            @stream = es.through (args) =>
                conn = es.readable (ct, callback) =>
                    return conn.emit 'end' if ct > 0
                    callback null, args
                @sut= parser(@stream)
                interpreter = es.through (reply) =>
                    @stream.emit 'data', reply
                    @stream.emit 'done' if @sut.done
                conn.pipe(es.split('\r\n'))
                    .pipe(@sut)
                    .pipe(interpreter)
        it 'should reply text', (done) ->
            ck = es.through (reply) ->
                reply.should.equal 'OK'
            @stream.on 'done', -> done()
            @stream.pipe(ck)
            @stream.write '+OK\r\n'
        it 'should reply integer', (done) ->
            ck = es.through (reply) ->
                reply.should.equal '3'
            @stream.on 'done', -> done()
            @stream.pipe(ck)
            @stream.write ':3\r\n'

    describe 'bulk reply', ->
        beforeEach ->
            @stream = es.through (args) =>
                conn = es.readable (ct, callback) =>
                    return conn.emit 'end' if ct > 0
                    callback null, args
                @sut= parser(@stream)
                interpreter = es.through (reply) =>
                    @stream.emit 'data', reply
                    @stream.emit 'done' if @sut.done
                conn.pipe(es.split('\r\n'))
                    .pipe(@sut)
                    .pipe(interpreter)
        it 'should reply data only', (done) ->
            ck = es.through (reply) ->
                reply.should.equal 'foobar'
            @stream.on 'done', -> done()
            @stream.pipe(ck)
            @stream.write '$6\r\nfoobar\r\n'

    describe 'multi-bulk reply', ->
        beforeEach ->
            @stream = es.through (args) =>
                conn = es.readable (ct, callback) =>
                    return conn.emit 'end' if ct > 0
                    callback null, args
                @sut= parser(@stream)
                interpreter = es.through (reply) =>
                    @stream.emit 'data', reply
                    @stream.emit 'done' if @sut.done
                conn.pipe(es.split('\r\n'))
                    .pipe(@sut)
                    .pipe(interpreter)
        it 'should reply data only', (done) ->
            replies = []
            ck = es.through (reply) ->
                replies.push reply

            @stream.on 'done', -> 
                replies.should.eql [
                    'foo'
                    'bar'
                    'Hello'
                    'World'
                ]
                done()
            @stream.pipe(ck)
            response = '*4\r\n'
            response+= '$3\r\nfoo\r\n'
            response+= '$3\r\nbar\r\n'
            response+= '$5\r\nHello\r\n'
            response+= '$5\r\nWorld\r\n'
            @stream.write response

    describe 'error reply', ->
        beforeEach ->
        it 'should emit error', (done) ->
            @stream = es.through (args) =>
                conn = es.readable (ct, callback) =>
                    return conn.emit 'end' if ct > 0
                    callback null, args
                @sut= parser(@stream)
                interpreter = es.through (reply) =>
                    @stream.emit 'data', reply
                    @stream.emit 'done' if @sut.done
                conn.on 'error', (err) ->
                    err.message.should.equal 'wrong data type'
                    done()
                conn.pipe(@sut)
                    .pipe(interpreter)
            @stream.write '-wrong data type'

            
