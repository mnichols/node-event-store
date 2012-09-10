es = require 'event-stream'
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
            eventStream = cfg.client.stream()
            xform = es.map (data, next) =>
                args = [
                    'zrangebyscore'
                    cfg.getCommitsKey(data.streamId)
                    data.streamRevision
                    data.streamRevision
                ]
                next null, args
            payload = es.map (data, next) =>
                return next null, [] unless data.payload
                next null, data.payload

            pipe = es.pipeline(
                auditStream,
                es.parse(),
                xform,
                eventStream,
                payload,
                es.writeArray
            )
            pipe






        new Admin cfg
            


