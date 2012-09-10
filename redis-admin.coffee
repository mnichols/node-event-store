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
                stream.readable = false
                ended = false
                destroyed = false
                inputs = 0
                outputs = 0
                
                stream.write = (data) ->
                    inputs++
                    data.forEach (e) ->
                        stream.emit 'data', e
                    outputs++
                    stream.emit 'drain'
                    stream.end() if ended


                stream.end = ->
                    ended = true
                    stream.writable = false
                    if inputs == outputs
                        stream.emit 'end'
                        stream.destroy()
                stream.destroy = -> 
                    destroyed = ended = true
                    stream.writable = false
                    process.nextTick ->
                        stream.emit 'close'
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
                return next() unless data.payload
                next null, data.payload

            pipe = es.pipeline(
                auditStream,
                es.parse(),
                xform,
                commitStream,
                es.parse(),
                payload,
                proxy()
            )
            originalWrite = pipe.write
            pipe.write = ->
                args = Array::slice.call @, arguments
                if args.length < 1
                    originalWrite [0, new Date(2999, 12,31).getTime()]
                else
                    originalWrite args[0]
            pipe

        new Admin cfg
            


