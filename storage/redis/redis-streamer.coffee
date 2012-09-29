es = require 'event-stream'
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

Redis::stream = (cmd, key, curry) ->
    curry = Array::slice.call arguments
    client = @
    selectCmd = parseCommand(['select', client.db])
    
    select = ->
        selected = false
        es.map (reply, next) ->
            return next null, reply+'' if selected
            selected = true
            next null, '~DB\r\n'

    stream = es.through (args) ->
        stream.pause()
        #accept arrays as data for `write`
        elems = concat [], stream.curry
        elems = concat elems, args
        parsed = parseCommand elems
        client.pool.acquire (err, conn) ->
            return stream.error err if err?
            conn.removeAllListeners()
            parseReply = replyParser stream, ->
                process.nextTick ->
                    client.pool.release conn
                stream.emit 'done'
                stream.resume()
            #conn.addListener 'error', stream.error
            replyPipe =
                es.pipeline(es.split('\r\n'),parseReply)

            #combine commands
            cmd = selectCmd +  parsed
            pipe = es.pipeline conn,
                replyPipe,
                es.through (reply) ->
                    stream.queue reply
            pipe.write cmd
    stream.curry = curry
    stream.error = (err) ->
        console.error 'redis-streamer', err
        stream.emit 'error', err
    stream






