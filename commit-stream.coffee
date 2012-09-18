{Stream} = require 'stream'
util = require 'util'

module.exports = (commit) ->
    unless commit
        throw new Error('commit is required')
    unless commit.streamId? and commit.streamRevision?
        throw new Error('commit must have streamId and streamRevision')
    commit.payload = commit.payload ? []
    inner = new Stream()
    inner.writable = true
    inner.readable = true
    ended = false
    destroyed = false
    paused = false
    inner.write = (data) ->
        if ended or destroyed
            throw new Error 'commit-stream no longer writable'
        return true unless data
        commit.streamRevision++
        commit.payload.push data
        return true
    inner.pause = ->
        paused = true
    inner.resume = ->
        paused = false
        inner.end() if ended
    inner.end = ->
        ended = true
        inner.writable = false
        return if paused
        ended = true
        inner.readable = false
        inner.emit 'data', commit
        inner.emit 'end'
        inner.emit 'close'

    inner.destroy = ->
        if destroyed
            throw new Error 'commit-stream already destroyed'

        inner.writable = inner.readable = false
        destroyed = true
        commit = null
        inner.emit 'end'
        inner.emit 'close'

    inner
