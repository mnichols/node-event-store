{Stream} = require 'stream'

ConcurrencyError = (message) ->
    Error.apply @, arguments
    @name = 'ConcurrencyError'
    @message = message ? 'ConcurrencyError'

ConcurrencyError:: = new Error()
ConcurrencyError::constructor = ConcurrencyError

buildThrough = ->
    stream = new Stream()
    stream.writable = true
    stream.readable = true
    stream.write = (commit) ->
        stream.commit = commit



#should just be readable
module.exports = (commit, opts) ->
    id = opts.getCommitsKey(commit.streamId)
    client = opts.client
    concurrencyArgs = ['zrevrange', id, 0, 0, 'WITHSCORES']
    redis = client.stream()
    score = null
    inflight = true
    ended = false
    paused = false
    destroyed = false
    replies = []
    stream = new Stream()
    stream.writable = false
    stream.readable = true

    #TODO implement read for starting concurrnecy read from redis
    stream.pause = ->
        paused = true

    stream.resume = ->
        paused = false
        validate()

    stream.end = ->
        return if ended or inflight or paused
        ended = true
        stream.emit 'end'
        stream.emit 'close'

    stream.destroy = ->
        return if destroyed
        stream.emit 'end'
        stream.emit 'close'
        ended = true

    redis.on 'data', (reply) ->
        replies.push reply ? 0
        inflight = replies.length <2 and reply
        validate()

    validate = ->
        return if ended or inflight
        redis.end()
        #last reply is the score
        score = Number(replies[replies.length-1])
        if score != commit.checkRevision
            err = new ConcurrencyError "Expected #{commit.checkRevision}, but got #{score}"
            console.error 'concurrency-stream', err
            stream.emit 'error', err
            return stream.end()
        stream.emit 'data', commit
        stream.end()

    process.nextTick ->
        redis.write concurrencyArgs
    stream


    


