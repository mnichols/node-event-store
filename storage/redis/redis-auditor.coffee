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
    createAuditor: (cfg) ->
        (cfg[k]=defaultCfg[k]) for k,v of defaultCfg when !cfg[k]?

        auditable = (commit) ->
            return commit && commit.timestamp && commit.streamId && commit.streamRevision
        writeAuditEntry = (commit, next) ->
            unless auditable(commit)
                console.error util.inspect commit
                return next new Error "commit is invalid for audit. " +
                    "timestamp, streamId, and streamRevision is required."

            process.nextTick =>
                auditEntry = 
                    streamId: commit.streamId
                    streamRevision: commit.streamRevision
                writeStream = @client.stream()
                writeStream.on 'data', (reply) ->
                    if Number(reply) != 1
                        #too late to do anything about it
                        @emit 'error',new Error "auditor failed with reply of #{reply}"
                    writeStream.end()
                writeStream.on 'error', (err) ->
                    @emit 'error', err
                writeArgs = [
                    'zadd'
                    @auditKey
                    new Date(commit.timestamp).getTime()
                    JSON.stringify auditEntry
                ]
                writeStream.write writeArgs
            #pass commit on thru
            next null, commit
        Auditor =  (cfg) ->
            EventEmitter2.call @
            (@[k]=cfg[k]) for k,v of cfg
            process.nextTick => @emit 'ready', null, @

        util.inherits Auditor, EventEmitter2
        
        Auditor::createAudit = ->
            es.map writeAuditEntry.bind @

        Auditor::createRangeStream = ->
            auditStream = cfg.client.stream 'zrangebyscore', @auditKey
            auditStream

        Auditor::createEventStream = ->
            commitCount = 0
            #default args...everything
            writeArgs = [0, new Date(2999, 12,31).getTime()]
            countStream = cfg.client.stream 'zcount', @auditKey
            countercept = es.map (data, next) ->
                    commitCount = Number(data)
                    console.log 'redis-auditor',"streaming #{commitCount} commits"
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

        new Auditor cfg
            


