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
            commitCount = 0
            #default args...everything
            writeArgs = [0, new Date(2999, 12,31).getTime()]
            countStream = cfg.client.stream 'zcount', 'streamId2RevByTime'
            countercept = es.map (data, next) ->
                    commitCount = Number(data)
                    console.log 'redis-admin',"streaming #{commitCount} commits"
                    next null, writeArgs
            auditStream = @createRangeStream()
            commitStream = cfg.client.stream()
            #@ this emits each event in the data array
            #ending the stream if the last commit packet has been received
            eachEvent = ->
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
                    if inputs>=commitCount
                        countStream.end()
                    stream.end() if ended


                stream.end = ->
                    ended = true
                    stream.writable = false
                    if inputs == commitCount
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
                countStream,
                countercept,
                auditStream,
                es.parse(),
                xform,
                commitStream,
                es.parse(),
                payload,
                eachEvent()
            )
            originalWrite = pipe.write
            pipe.write = ->
                args = Array::slice.call arguments
                writeArgs = args[0] if args.length > 0
                originalWrite writeArgs
            pipe

        new Admin cfg
            


