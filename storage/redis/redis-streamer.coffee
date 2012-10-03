es = require 'event-stream'
net = require 'net'
replyParser = require './redis-reply-parser'
parseCommand = require './redis-command-parser'
connectionPool = require './connection-pool'

defaultCfg = 
    port: 6379
    host: '127.0.0.1'
    db: 0
    pool: null
    maxConnections: 15
module.exports.Redis = Redis = (cfg) ->
    (cfg[k]=defaultCfg[k]) for k,v of defaultCfg when !cfg[k]
    (@[k]=cfg[k]) for k,v of cfg
    @pool = @pool ? connectionPool cfg
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
        cb null, parsed
    
### pooled
Redis::stream = (cmd, key, curry) ->
    curry = Array::slice.call arguments
    passes = -1
    conn = null #@createConnection()
    select = parseCommand(['select', @db])
    stream = es.through (args) =>
        stream.pause()
        xform = es.map @formatCommand curry, select
        init = es.map (cmd, next) =>
            @pool.acquire (err, connection) ->
                init.emit 'error', err if err?
                conn =connection
                conn.removeAllListeners()
                next null, cmd

        commander = es.through (cmd) ->
            conn.on 'data', (data) ->
                commander.emit 'data', data
            conn.on 'error', (err) ->
                commander.emit 'error', err
            conn.write cmd


        pluckSelect = 
            es.map (reply, next) ->
                #clips first reply which is the select db cmd
                passes++
                return next() unless passes
                next null, reply
        
        execute = es.pipeline(es.pipeline(init,
            commander,
            es.split('\r\n')),
            pluckSelect)


        reply = replyParser => 
            stream.emit 'done'
            passes = -1
            @pool.release conn
            stream.resume()
        reply.on 'data', ->
            args = Array::slice.call arguments
            args.unshift 'data'
            stream.emit.apply stream, args
        reply.on 'error', stream.error
        command = es.pipeline xform, execute, reply

        command.write args
        
    stream.error = (err) ->
        console.error 'redis-streamer', err
        stream.emit 'error', err

    stream
###
#
#notpooled
#Redis::stream = (cmd, key, curry) ->
#    stream = null
#    curry = Array::slice.call arguments
#    passes = -1
#    conn = es.through (ignore) ->null #@createConnection()
#    select = parseCommand(['select', @db])
#
#    xform = es.map @formatCommand curry, select
#    pluckSelect = 
#        es.map (reply, next) ->
#            #clips first reply which is the select db cmd
#            passes++
#            return next() unless passes
#            next null, reply
#
#    execute = es.pipeline(es.pipeline(conn, 
#        es.split('\r\n')),
#        pluckSelect)
#
#    command = es.pipeline xform, execute
#
#    reply = replyParser -> 
#        stream.emit 'done'
#        passes = -1
#
#    stream =
#        es.pipeline(command, reply)
#    stream.error = (err) ->
#        console.error 'redis-streamer', err
#        stream.emit 'error', err
#
#    stream


Redis::stream = (cmd, key, curry) ->
    stream = null
    curry = Array::slice.call arguments
    passes = -1
    conn = @pool.createProxy()
    select = parseCommand(['select', @db])

    init = es.map (args, next) ->
        conn.connect (err) =>
            console.error err if err?
            next err if err?
            next null, args

    xform = es.map @formatCommand curry, select
    pluckSelect = 
        es.map (reply, next) ->
            #clips first reply which is the select db cmd
            passes++
            return next() unless passes
            next null, reply

    execute = es.pipeline(es.pipeline(init, conn, 
        es.split('\r\n')),
        pluckSelect)

    command = es.pipeline xform, execute

    reply = replyParser -> 
        conn.release ->
            stream.emit 'done'
            passes = -1
    stream = es.pipeline command, reply
    stream.error = (err) ->
        console.error 'redis-streamer', err
        stream.emit 'error', err

    stream
