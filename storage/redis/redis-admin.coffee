es = require 'event-stream'
{Stream} = require('stream')
Redis = require 'redis-stream'
{EventEmitter2} = require 'eventemitter2'
util = require 'util'
defaultCfg = 
    id: 'redis-storage'
    client: null
    getCommitsKey: (streamId) -> "commits:#{streamId}"
    auditKey: 'streamIdRevByTime'

module.exports = 
    createAdmin: (cfg) ->
        (cfg[k]=defaultCfg[k]) for k,v of defaultCfg when !cfg[k]?
        Admin =  (cfg) ->
            EventEmitter2.call @
            (@[k]=cfg[k]) for k,v of cfg
            process.nextTick => @emit 'ready', null, @
        util.inherits Admin, EventEmitter2
        Admin::audit = (commit) ->
            process.nextTick =>
                map = 
                    streamId: commit.streamId
                    streamRevision: commit.streamRevision
                auditor = cfg.client.stream()
                auditor.write [
                    'zadd'
                    @auditKey
                    commit.timestamp.getTime()
                    JSON.stringify map
                ]
                auditor.end()

        Admin::createRangeStream = ->
            auditStream = cfg.client.stream 'zrangebyscore', @auditKey
            auditStream

        Admin::createEventStream = ->
            commitCount = 0
            #default args...everything
            writeArgs = [0, new Date(2999, 12,31).getTime()]
            countStream = cfg.client.stream 'zcount', @auditKey
            countercept = es.map (data, next) ->
                    commitCount = Number(data)
                    console.log 'redis-admin',"streaming #{commitCount} commits"
                    next null, writeArgs
            auditStream = @createRangeStream()
            commitStream = cfg.client.stream()
            eachEvent = (require './each-event-stream')()
            eachEvent.on 'tick', (inputs) ->
                if inputs >= commitCount
                    countStream.end()

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
                countStream,
                countercept,
                auditStream,
                es.parse(),
                xform,
                commitStream,
                es.parse(),
                payload,
                eachEvent
            )
            originalWrite = pipe.write
            pipe.write = ->
                args = Array::slice.call arguments
                writeArgs = args[0] if args.length > 0
                originalWrite writeArgs
            pipe

        new Admin cfg
            


