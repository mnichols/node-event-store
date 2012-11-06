ReplyParser = require '../redis-reply-parser'
ReadableArray = require '../readable-array'
SplitStream = require '../split-stream'
Duplex = require '../node_modules/readable-stream/duplex'
Transform = require '../node_modules/readable-stream/transform'
Writable = require '../node_modules/readable-stream/writable'
Readable = require 'readable-stream'
{StringDecoder} = require 'string_decoder'
sd = new StringDecoder 'utf8'
util = require 'util'

Ck = (assertion) ->
    Writable.apply @
    @_write = (chunk, cb=->) ->
        assertion(sd.write(chunk))
        cb()
    @
util.inherits Ck, Writable

describe 'redis-reply-parser', ->
    es = require 'event-stream'
    parser = require '../redis-reply-parser'
    describe 'single-line reply', ->
        beforeEach ->
            #            @stream = es.through (args) =>
            #                conn = es.readable (ct, callback) =>
            #                    return conn.emit 'end' if ct > 0
            #                    callback null, args
            #                @sut= parser(@stream)
            #                interpreter = es.through (reply) =>
            #                    @stream.emit 'data', reply
            #                    @stream.emit 'done' if @sut.done
            #                conn.pipe(es.split('\r\n'))
            #                    .pipe(@sut)
            #                    .pipe(interpreter)
        it 'should reply text', (done) ->
            arr = new ReadableArray ['+OK']
            ck = new Ck (reply) ->
                reply.should.equal 'OK'
                done()
            parser = new ReplyParser()
            arr.pipe(parser).pipe(ck)

#            ck = es.through (reply) ->
#                reply.should.equal 'OK'
#            @stream.on 'done', -> done()
#            @stream.pipe(ck)
#            @stream.write '+OK\r\n'
        it 'should reply integer', (done) ->
            arr = new ReadableArray [':3']
            ck = new Ck (reply) ->
                reply.should.equal '3'
                done()
            parser = new ReplyParser()
            arr.pipe(parser).pipe(ck)

#            ck = es.through (reply) ->
#                reply.should.equal '3'
#            @stream.on 'done', -> done()
#            @stream.pipe(ck)
#            @stream.write ':3\r\n'

    describe 'bulk reply', ->
        beforeEach ->
            #            @stream = es.through (args) =>
            #                conn = es.readable (ct, callback) =>
            #                    return conn.emit 'end' if ct > 0
            #                    callback null, args
            #                @sut= parser(@stream)
            #                interpreter = es.through (reply) =>
            #                    @stream.emit 'data', reply
            #                    @stream.emit 'done' if @sut.done
            #                conn.pipe(es.split('\r\n'))
            #                    .pipe(@sut)
            #                    .pipe(interpreter)
        it 'should reply data only', (done) ->
            arr = new ReadableArray ['$6\r\nfoobar\r\n']
            ck = new Ck (reply) ->
                reply.should.equal 'foobar'
                done()
            parser = new ReplyParser()
            split = new SplitStream()
            arr.pipe(parser).pipe(ck)

#            ck = es.through (reply) ->
#                reply.should.equal 'foobar'
#            @stream.on 'done', -> done()
#            @stream.pipe(ck)
#            @stream.write '$6\r\nfoobar\r\n'

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
            #replies = []
            #ck = es.through (reply) ->
            #    replies.push reply

            #@stream.on 'done', -> 
            #    replies.should.eql [
            #        'foo'
            #        'bar'
            #        'Hello'
            #        'World'
            #    ]
            #    done()
            #@stream.pipe(ck)
            #response = '*4\r\n'
            #response+= '$3\r\nfoo\r\n'
            #response+= '$3\r\nbar\r\n'
            #response+= '$5\r\nHello\r\n'
            #response+= '$5\r\nWorld\r\n'
            #@stream.write response
            response = '*4\r\n'
            response+= '$3\r\nfoo\r\n'
            response+= '$3\r\nbar\r\n'
            response+= '$5\r\nHello\r\n'
            response+= '$5\r\nWorld\r\n'
            replies = [
                    'foo'
                    'bar'
                    'Hello'
                    'World'
                ]
            arr = new ReadableArray [response]
            ck = new Ck (reply) ->
                reply.should.equal replies.slice()
                done() if replies.length==0
            parser = new ReplyParser()
            arr.pipe(parser).pipe(ck)

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

    describe 'special replies', ->
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

        it 'should emit null for non-existing bulk value', (done) ->
            ck = es.through (reply) ->
                expect(reply).to.be.null
            @stream.on 'done', -> 
                done()
            @stream.pipe(ck)
            @stream.write '$-1\r\n'
        it 'should emit null for non-existing multi value', (done) ->
            ck = es.through (reply) ->
                expect(reply).to.be.null
            @stream.on 'done', -> 
                done()
            @stream.pipe(ck)
            @stream.write '*-1\r\n'
        it 'should emit empty string for non-existing key', (done) ->
            ck = es.through (reply) ->
                reply.should.equal ''
            @stream.on 'done', -> 
                done()
            @stream.pipe(ck)
            @stream.write '*0\r\n'


            
