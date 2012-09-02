es = require 'event-stream'
createCommit = (cfg) ->
    checkRevision: cfg.streamRevision ? 0
    streamId: cfg.streamId        
    streamRevision: (cfg.streamRevision ? 0) + cfg.events.length
    timestamp: new Date().getTime()
    headers: []
    payload: cfg.events

    
module.exports = (stream, storage) ->
    streamId = stream.streamId
    uncommitted = []
    commitStream = es.map (data, callback) ->
        storage.write data, (err, result) ->
            return callback err if err
            callback null, result

    addEvents = (events = []) ->
        uncommitted = uncommitted.concat events
    
    commitStream.commit = (events = []) ->
        addEvents events if events
        cfg =
            streamRevision: stream.streamRevision
            streamId: streamId
            events: uncommitted
        commitStream.write createCommit(cfg)
        commitStream.end()
    commitStream


