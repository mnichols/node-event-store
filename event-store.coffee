es = require 'event-stream'
committable = require './committable'
proxy = require './proxy-stream'
module.exports = (storage, auditor) ->
    open: (filter) ->
        stream = storage.createReader filter
        (stream[k]=filter[k]) for k,val of filter

        proxyCommit = proxy()
        stream.commit =  proxyCommit
        
        stream.on 'end', ->
            #streams are not committable until they 
            #have been read
            commit = committable stream, storage
            if auditor 
                audit = auditor.createAudit()                
                commit.pipe audit
            proxyCommit.enable commit
        return stream

