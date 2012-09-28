es = require 'event-stream'
replyParser = require './redis-reply-parser'
parseCommand = require './redis-command-parser'
connectionPool = require './connection-pool'

defaultCfg = 
    port: 6379
    host: '127.0.0.1'
    db: 0
    pool: null
    maxConnections: 25
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

replyHandler = (stream, replyState) ->
    es.through (reply) ->
        stream.emit 'data', reply
        stream.emit 'done' if replyState.done
Redis::stream = (cmd, key, curry) ->
    curry = Array::slice.call arguments
    client = @
    stream = es.through (args) ->
        stream.pause()
        #accept arrays as data for `write`
        elems = concat [], stream.curry
        elems = concat elems, args
        parsed = parseCommand elems
        release = (conn) ->
            releaseBackToPool = -> 
                client.pool.release(conn)
                stream.db = null
                conn.removeAllListeners()
                stream.resume()

        
        client.pool.acquire (err, conn) ->
            return stream.error err if err?
            stream.selectDb conn, ->
                stream.release = release conn
                parseReply = replyParser stream
                handleReply = replyHandler stream, parseReply
                conn.pipe(es.split('\r\n'))
                    .pipe(parseReply)
                    .pipe(handleReply)
                conn.addListener 'error', stream.error
                conn.write parsed
    stream.selectDb = (conn, cb) ->
        #drop any reply from db select
        ignoreSelectReply = es.through (reply) ->
            conn.removeAllListeners()
            cb null, conn
        conn.pipe(ignoreSelectReply)
        conn.write(parseCommand(['select', client.db]))
        stream.db = client.db
    stream.curry = curry
    stream.error = (err) ->
        console.error 'redis-streamer', err
        stream.emit 'error', err
    stream.addListener 'done', -> 
        stream.release()
    stream






