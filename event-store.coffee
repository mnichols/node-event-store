es = require 'event-stream'
committable = require './committable'
module.exports = (storage, auditor) ->
    open: (filter) ->
        stream = storage.createReader filter
        (stream[k]=filter[k]) for k,val of filter
        stream.on 'end', ->
            commit = committable stream, storage
            if auditor
                commit = es.pipeline(commit, auditor.audit)
            stream.commit = commit
        return stream

