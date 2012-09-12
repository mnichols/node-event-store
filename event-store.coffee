mapStream = require 'map-stream'
committable = require './committable'
module.exports = (storage) ->
    open: (filter) ->
        stream = storage.createReader filter
        (stream[k]=filter[k]) for k,val of filter
        stream.on 'end', ->
            stream.commit = committable(stream, storage)
        return stream

