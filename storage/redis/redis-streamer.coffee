es = require 'event-stream'
net = require 'net'
replyParser = require './redis-reply-parser'
parseCommand = require './redis-command-parser'
connectionPool = require './connection-pool'
MuxDemux = require 'mux-demux'

defaultCfg = 
    port: 6379
    host: '127.0.0.1'
    db: 0
    pool: null
    maxConnections: 20
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
Redis::stream = (cmd, key, curry) ->
    curry = Array::slice.call arguments
    client = @
    passes = -1
    conn = @createConnection()
    selectCmd = parseCommand(['select', client.db])
    stream = null
    xform = es.map (args, next) ->
        passes = -1
        #accept arrays as data for `write`
        elems = concat [], stream.curry
        elems = concat elems, args
        parsed = parseCommand elems
        #select db
        parsed = selectCmd + parsed
        next null, parsed

    
    pluckSelect = 
        es.map (reply, next) ->
            passes++
            return next() unless passes
            next null, reply

    execute = es.pipeline(es.pipeline(conn, 
        es.split('\r\n')),
        pluckSelect)

    command = es.pipeline xform, execute
#    execute = es.map (cmd, next) ->
#        client.pool.acquire (err, conn) ->
#            return next err if err?
#            conn.removeAllListeners()
#            conn.addListener 'error', stream.error
#            cmd = selectCmd +  cmd
#            passes = -1
#            thru = es.through (reply) ->
#                passes++
#                return unless passes
#                selected= true
#                next null, reply
#            conn.pipe(es.split('\r\n')).pipe(thru)
#            conn.write cmd

    reply = replyParser -> 
        passes = -1
        stream.emit 'done'
    stream =
        es.pipeline(es.pipeline(xform, execute), reply)
    stream.curry = curry
    stream.error = (err) ->
        console.error 'redis-streamer', err
        stream.emit 'error', err

    stream


