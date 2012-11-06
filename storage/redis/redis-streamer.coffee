es = require 'event-stream'
net = require 'net'
replyParser = require './redis-reply-parser'
parseCommand = require './redis-command-parser'
Duplex = require './node_modules/readable-stream/duplex'
Transform = require './node_modules/readable-stream/transform'

defaultCfg = 
    port: 6379
    host: '127.0.0.1'
    db: 0
    maxConnections: 15
module.exports.Redis = Redis = (cfg) ->
    (cfg[k]=defaultCfg[k]) for k,v of defaultCfg when !cfg[k]
    (@[k]=cfg[k]) for k,v of cfg
    @

concat = (target, data) ->
    target?=[]
    unless Array.isArray(data)
        data = [data]
    Array::push.apply target, data
    target

Redis::createConnection = ->
    net.createConnection @port, @host
Redis::formatCommand = (curry=[], before = '') ->
    (args = [], cb) ->
        #accept arrays as data for `write`
        elems = concat [], curry
        elems = concat elems, args
        parsed = parseCommand elems
        #select db
        parsed = before + parsed
        return parsed

Redis::stream = (cmd, key, curry) ->
    stream = null
    curry = Array::slice.call arguments
    passes = -1
    conn = @createConnection()
    select = parseCommand(['select', @db])

    class Xform extends Transform
        constructor: (@curry=[]) ->
            super {lowWaterMark: 0}
        _transform: (args=[], outputFn, cb) ->
            #accept arrays as data for `write`
            cmd = if @curry.length then  parseCommand(concat [], @curry) else ''
            elems = if cmd.length then new Buffer(cmd) else new Buffer(0)
            elems = Buffer.concat [elems, args]
            #parsed = new Buffer(parseCommand elems)
            #select db
            parsed = Buffer.concat [ new Buffer(select), elems]
            outputFn parsed
            cb()

    class Pluck extends Transform
        constructor: ->
            super {lowWaterMark: 0}
        _transform: (reply, outputFn, cb) ->
            unless passes
                reply = new Buffer(reply.toString().replace(select, ''))
                passes++
            outputFn reply if reply.length>0
            cb()

    class Split extends Transform
        constructor: ->
            super {lowWaterMark: 0}
        _transform: (chunk, outputFn, cb) ->
            outputFn(new Buffer(data)) for data in chunk.toString().split('\r\n')
            cb()


    #xform = es.map @formatCommand curry, select
#    pluckSelect = 
#        es.map (reply, next) ->
#            #clips first reply which is the select db cmd
#            passes++
#            return next() unless passes
#            next null, reply
    xform = new Xform curry
    pluckSelect = new Pluck()
    split = new Split()

    
    xform.pipe(conn).pipe(split).pipe(pluckSelect)

    return xform
    
    execute = es.pipeline(es.pipeline(conn, 
        es.split('\r\n')),
        pluckSelect)

    command = es.pipeline xform, execute

    reply = replyParser -> 
        stream.emit 'done'
        passes = -1
    stream = es.pipeline command, reply
    stream.error = (err) ->
        console.error 'redis-streamer', err
        stream.emit 'error', err

    stream
