mapStream = require 'map-stream'
committable = require './committable'
module.exports = (storage) ->
    storage.on "#{storage.id}.commit", (commit) ->
        process.nextTick ->
            console.log 'queuing commit', commit
    open: (filter) ->
        stream = storage.createReader filter
        (stream[k]=filter[k]) for k,val of filter
        stream.on 'end', ->
            stream.commit = committable(stream, storage)
        return stream

