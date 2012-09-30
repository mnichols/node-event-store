es = require 'event-stream'
pipeline = require './pipeline'
{Stream} = require('stream')
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
            #default args...everything
            writeArgs = [0, new Date(2999, 12,31).getTime()]
            commitCount = 0
            inputs = 0

            countStream = cfg.client.stream 'zcount', @auditKey
            countercept = es.map (data, next) ->
                commitCount = Number(data)
                console.log 'redis-auditor',"streaming #{commitCount} commits"
                next null, writeArgs


            reader = cfg.eventStorage.createReader()

            nextCommit = ->
            oneCommitAtATime = es.map (auditEntry, next) =>
                unless auditEntry.streamId and auditEntry.streamRevision
                    throw new Error 'invalid audit entry'
                
                filter = 
                    streamId: auditEntry.streamId
                    minRevision: Number(auditEntry.streamRevision)
                    maxRevision: Number(auditEntry.streamRevision)
                readerOpts =
                    enrich: false
                    flatten: true
                    emitStreamHeader: false
                reader.read filter,readerOpts, -> next()

            stream = pipeline(
                countStream,
                countercept,
                @createRangeStream(),
                es.parse(),
                oneCommitAtATime)
            reader.on 'error', (err) -> stream.emit 'error', err
            reader.on 'data', ->
                args = Array::slice.call arguments
                args.unshift 'data'
                stream.emit.apply stream, args
            reader.on 'done', (count) ->
                inputs++
                if inputs>=commitCount
                    reader.destroy()
                    return stream.emit 'end'


            stream.read = ->
                args = Array::slice.call arguments
                writeArgs = args[0] if args.length > 0
                stream.write writeArgs

            stream

        new Auditor cfg
            


