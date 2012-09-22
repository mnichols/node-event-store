es = require 'event-stream'
committable = require './committable'
module.exports = (storage, auditor) ->
    open: (filter) ->
        stream = storage.createReader filter
        (stream[k]=filter[k]) for k,val of filter
        stream.commit =  (cfg = {}) ->
            #streams are not committable until they 
            #have been read
            commit = committable stream, storage, cfg
            if auditor 
                audit = auditor.createAudit()                
                commit.pipe audit
            return commit
        
        stream.on 'end', -> stream.committable = true
        return stream

