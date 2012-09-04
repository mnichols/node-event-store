es = require 'event-stream'
Redis = require 'redis-stream'
{EventEmitter} = require 'events'
util = require 'util'

getEventsKey =  (streamId) -> "events:#{streamId}"
module.exports = 
    createStorage: (client, cb) ->
        ConcurrencyError = ->
            Error.apply @, arguments
            @name = 'ConcurrencyError'

        ConcurrencyError:: = new Error()
        ConcurrencyError::constructor = ConcurrencyError
        isNumber = (obj) ->
            toString.call(obj)=='[object Number]'

        Storage = ->
            EventEmitter.call @
            util.inherits Storage, EventEmitter
            read: (filter, callback) ->
                id = getEventsKey(filter.streamId)
                reader = client.stream()
                err = null
                events = []
                push = (moreEvents=[]) -> Array::push.apply events, moreEvents
                reader.on 'end', -> 
                    callback err, events
                reader.on 'data', (data) ->
                    obj = JSON.parse data
                    push obj.payload

                reader.on 'error', (error) -> 
                    console.error error
                    err = error
                reader.redis.write Redis.parse [
                    'zrangebyscore'
                    id
                    filter.minRevision
                    filter.maxRevision ? -1
                ]
                reader.end()


            writeStream: (commit, callback) ->
                id = getEventsKey(commit.streamId)
                reply = null
                concurrency = client.stream 'zrevrange', id, 0, 1
                check = (data, next) ->
                    return next data if ~data.indexOf '-ERR'
                    score = Number(data)
                    #first record is likely the actual object
                    return next() if isNaN score 
                    if score == commit.checkRevision
                        return next null, JSON.stringify(commit)
                    next new ConcurrencyError()
                writer = client.stream('zadd', id, commit.streamRevision)
                respond = (data, next) ->
                    reply = JSON.parse data
                    next null, reply

                pipe = es.pipe(concurrency, es.map(check), writer, es.map(respond))

                pipe.on 'error', callback
                pipe.on 'end', -> callback null, reply
                pipe.write 'WITHSCORES'
                pipe.end()
            createCommitter: ->
                emitter = es.map (commit, next) ->
                    id = getEventsKey(commit.streamId)
                    writer = client.stream('zadd',id,commit.streamRevision)
                    maxRevision = client.stream('zrevrange',id,0,1)
                    validate = es.map (data, next) ->
                        score = Number(data)
                        #first record is likely the actual object
                        #so just drop this data
                        return next() if isNaN score 
                        if score==commit.checkRevision
                            return next null, null
                        next new ConcurrencyError()
                    _commit = (data, next) ->
                        delete commit.checkRevision
                        emitter.emit 'commit', commit
                        emitter.emit 'data', commit #for piping
                    _write = ->
                        pipe = es.pipeline(writer, es.map(_commit))
                        pipe.write JSON.stringify commit
                        pipe.end()
                    _error = (err) -> 
                        validate.removeListener 'end', _write
                        validate.destroy()
                        emitter.emit 'error', err

                    validate.on 'error',  _error
                    maxRevision.on 'error', _error
                    writer.on 'error', _error
                    validate.on 'end', _write
                    ck = es.pipeline(
                        maxRevision,
                        validate)
                    ck.write 'WITHSCORES'
                    ck.end()
                    

            write: (commit, callback) ->
                id = getEventsKey(commit.streamId)
                concurrency = client.stream()
                check = (data, next) ->
                    score = Number(data)
                    #first record is likely the actual object
                    return if isNaN score 
                    return next null, null if score == commit.checkRevision
                    next new ConcurrencyError()

                
                writeCommit = (data, next) ->
                    reply = null
                    writer = client.stream 'zadd', id, commit.streamRevision
                    writer.on 'data', (data) ->
                        reply = JSON.parse data
                    writer.on 'end', ->
                        callback null, reply
                    #dont need to persist our check
                    delete commit.checkRevision
                    writer.write JSON.stringify commit
                    writer.end()

                finish = writeCommit

                concurrency.on 'data', (reply) ->
                    score = Number(reply)
                    #first record is likely the actual object
                    return if isNaN score 
                    return if score == commit.checkRevision
                    finish = -> callback new ConcurrencyError()
                
                concurrency.on 'end', ->
                    finish()

                concurrency.redis.write Redis.parse [
                    'zrevrange'
                    id
                    0
                    1
                    'WITHSCORES'
                ]
                concurrency.end()
        cb null, new Storage()

