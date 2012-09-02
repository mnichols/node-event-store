es = require 'event-stream'
committable = require './committable'
module.exports = (storage) ->
    open: (filter) ->
        stream =
            es.map (filter, callback) ->
                unless filter.streamId
                    return callback new Error 'streamId is required'

                storage.read filter, (err, result) ->
                    return callback err if err
                    (filter[k]=result[k]) for k,val of result
                    #stream is not committable until events have been `read`
                    stream.makeCommittable = -> committable stream, storage
                    callback null, filter
        (stream[k]=filter[k]) for k,val of filter
        stream.read = -> 
            stream.write filter
            stream.end()
        stream

