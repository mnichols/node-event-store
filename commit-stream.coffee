{Stream} = require 'stream'
util = require 'util'

module.exports = (commit) ->
    unless commit
        throw new Error('commit is required')
    unless commit.streamId? and commit.streamRevision?
        throw new Error('commit must have streamId and streamRevision')
    commit.payload = commit.payload ? []
    stream = new Stream()
    stream.writable = true
    stream.readable = true
    ended = false
    destroyed = false
    paused = false
    stream.write = (data) ->
        if ended or destroyed
            throw new Error 'commit-stream no longer writable'
        return true unless data
        commit.streamRevision++
        commit.payload.push data
        return true
    stream.pause = ->
        paused = true
    stream.resume = ->
        paused = false
        stream.end() if ended
    stream.on 'end', ->
        stream.readable = false
        stream.emit 'data', commit
        stream.readable = false
    stream.end = ->
        return if ended
        ended = true
        stream.writable = false
        return if paused
        stream.emit 'end'
        stream.emit 'close'

    stream.destroy = ->
        if destroyed
            throw new Error 'commit-stream already destroyed'
        stream.writable = stream.readable = false
        destroyed = true
        commit = null
        stream.emit 'end'
        stream.emit 'close'

    stream
