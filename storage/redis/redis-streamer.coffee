es = require 'event-stream'
replyParser = require './redis-reply-parser'
parseCommand = require './redis-command-parser'
connectionPool = require './connection-pool'

defaultCfg = 
    port: 6379
    host: 'localhost'
    db: 0
    pool: -> connectionPool @
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

replyHandler = (stream, replyState) ->
    es.through (reply) ->
        stream.emit 'data', reply
        stream.emit 'done' if replyState.done
Redis::stream = (cmd, key, curry) ->
    curry = Array::slice.call arguments
    client = @
    stream = es.through (args) ->
        #accept arrays as data for `write`

        stream.pause()
        elems = concat [], stream.curry
        elems = concat elems, args
        parsed = parseCommand elems
        connPool = client.pool()
        release = (conn) ->
            releaseBackToPool = -> 
                connPool.release conn
                conn.removeListener 'error', stream.error
                stream.removeListener 'done', releaseBackToPool
                stream.removeListener 'error', releaseBackToPool
                stream.resume()

#        stream.inflight++
#        stream.pause() if connPool.max == stream.inflight++
        
        connPool.acquire (err, conn) ->
            return stream.error err if err?
            stream.selectDb conn
            stream.addListener 'done', release(conn)
            parseReply = replyParser stream
            handleReply = replyHandler stream, parseReply
            conn.pipe(es.split('\r\n'))
                .pipe(parseReply)
                .pipe(handleReply)
            conn.addListener 'error', stream.error
            conn.write parsed
    stream.selectDb = (conn) ->
        return if stream.db == client.db
        stream.db = client.db
        conn.write(parseCommand(['select', client.db]))
    stream.curry = curry
    stream.error = (err) ->
        console.error 'redis-streamer', err
        stream.emit 'error', err
    stream.inflight = 0
    stream






