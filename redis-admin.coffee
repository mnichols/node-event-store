es = require 'event-stream'
{Stream} = require('stream')
Redis = require 'redis-stream'
{EventEmitter2} = require 'eventemitter2'
util = require 'util'
defaultCfg = 
    id: 'redis-storage'
    client: null
    getCommitsKey: (streamId) -> "commits:#{streamId}"

module.exports = 
    createAdmin: (cfg) ->
        (cfg[k]=defaultCfg[k]) for k,v of defaultCfg when !cfg[k]?
        Admin =  ->
            EventEmitter2.call @
            process.nextTick => @emit 'ready', null, @
        util.inherits Admin, EventEmitter2
        Admin::createRangeStream = ->
            auditStream = cfg.client.stream 'zrangebyscore', 'streamId2RevByTime'
            auditStream

        Admin::createEventStream = ->
            auditStream = @createRangeStream()
            commitStream = cfg.client.stream()
            proxy = ->
                stream = new Stream()
                stream.writable = true
                stream.readable = true
                ended = false
                paused = false
                
                stream.write = (data, next) ->
                    data.forEach (e) ->
                        stream.emit 'data', e
                stream.end = -> stream.emit 'end'
                stream.destroy = -> 
                    stream.emit 'end'
                    stream.emit 'close'
                    ended = true
                stream
                

            xform = es.map (data, next) =>
                args = [
                    'zrangebyscore'
                    cfg.getCommitsKey(data.streamId)
                    data.streamRevision
                    data.streamRevision
                ]
                next null, args
            payload = es.map (data, next) =>
                return next null, null unless data.payload
                next null, data.payload

            inner = es.pipeline(
                auditStream,
                es.parse(),
                xform,
                commitStream,
                es.parse(),
                payload,
                proxy()
            )
            inner

        new Admin cfg
            


