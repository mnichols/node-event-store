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
Redis::formatCommand = (curry=[], before = '') ->
    (args = [], cb) ->
        #accept arrays as data for `write`
        elems = concat [], curry
        elems = concat elems, args
        parsed = parseCommand elems
        #select db
        parsed = before + parsed
        cb null, parsed
    
Redis::stream = (cmd, key, curry) ->
    stream = null
    curry = Array::slice.call arguments
    passes = -1
    conn = @createConnection()
    select = parseCommand(['select', @db])
    xform = es.map @formatCommand curry, select
    pluckSelect = 
        es.map (reply, next) ->
            #clips first reply which is the select db cmd
            passes++
            return next() unless passes
            next null, reply

    execute = es.pipeline(es.pipeline(conn, 
        es.split('\r\n')),
        pluckSelect)

    command = es.pipeline xform, execute

    reply = replyParser -> 
        stream.emit 'done'
        passes = -1
    stream =
        es.pipeline(command, reply)
    stream.error = (err) ->
        console.error 'redis-streamer', err
        stream.emit 'error', err

    stream


