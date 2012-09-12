{Stream} = require 'stream'
###@ this emits each event in the data array,
###
module.exports = (target) ->
    stream = new Stream()
    stream.writable = true
    stream.readable = true
    ended = false
    destroyed = false
    inputs = 0
    outputs = 0
    
    stream.write = (data = []) ->
        unless Object::toString.call data == '[object Array]'
            stream.emit 'error', new Error('data must be an array')
            stream.end()
        inputs++
        data.forEach (e, i) ->
            stream.emit 'data', e
        stream.emit 'tick', inputs

    stream.end = ->
        return if ended
        ended = true
        stream.writable = false
        inputs = 0
        stream.emit 'end'
        stream.destroy()
    stream.destroy = -> 
        destroyed = ended = true
        stream.writable = false
        process.nextTick ->
            stream.emit 'close'
    stream

