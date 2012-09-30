{Stream} = require 'stream'
es = require 'event-stream'
pipeline = require './pipeline'
util = require 'util'
ConcurrencyError = (msg) ->
    Error.captureStackTrace @, @
    @message = msg || 'ConcurrencyError'

util.inherits ConcurrencyError, Error
ConcurrencyError::name = 'ConcurrencyError'
module.exports = (opts = {}) ->
    unless opts.client?
        throw new Error 'redis client is required'
    client = opts.client
    replies = []
    commit = null
    redis = client.stream()
    reset = -> 
        commit = null
        replies = []
    init = es.map (theCommit, next) ->
        replies = []
        commit?=theCommit
        next null, commit
    xform = es.map (commit, next) ->
        id = opts.getCommitsKey(commit.streamId)
        concurrencyArgs = ['zrevrange', id, 0, 0, 'WITHSCORES']
        next null, concurrencyArgs
        
    validate = es.map (reply, next) ->
        replies.push reply
        #empty reply means the key doesnt exist yet, so PASS
        return unless reply=='' or replies.length > 1
        score = Number(reply)
        if score == commit.checkRevision
            return next null, commit
        message =  "Expected #{commit?.checkRevision}, but got #{score}"
        err = new ConcurrencyError message
        console.error 'concurrency-stream', err
        next err

    done = es.map (output, next) ->
        reset()
        next null, output

    stream = pipeline init,
        xform,
        redis, 
        validate,
        done
    stream
