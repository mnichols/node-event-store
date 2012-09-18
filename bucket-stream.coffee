{Stream} = require 'stream'
module.exports = (buffer = []) ->
    stream = new Stream()
    stream.writable = true
    stream.readable = true
    paused = false
    ended = false

    stream.write = (event) ->
        #apply to Aggregate here
        buffer.push event
        return true
    _read = ->
        stream.emit 'data', buffer
        buffer = []
    stream.on 'end', ->
        process.nextTick ->
            stream.readable = false
    stream.end = ->
        return if ended
        stream.writable = false
        ended = true
        _read()
        buffer = []
        stream.emit 'end'
        stream.emit 'close'
    stream.destroy = ->
        ended = true
        stream.emit 'end'
        stream.emit 'close'
    stream.pause = ->
        paused = true
    stream.resume = ->
        paused = false
        stream.end() if stream.readable
    stream

