es = require 'event-stream'
createCommit = (cfg) ->
    now = new Date()
    utc = Date.UTC now.getFullYear(), 
        now.getMonth(), 
        now.getDate(), 
        now.getHours(), 
        now.getMinutes(), 
        now.getSeconds(),
        now.getMilliseconds()
    checkRevision: cfg.streamRevision ? 0
    streamId: cfg.streamId        
    streamRevision: (cfg.streamRevision ? 0) + cfg.events.length
    timestamp: utc
    headers: []
    payload: cfg.events

    
module.exports = (cfg, storage) ->
    unless cfg.streamId
        throw new Error 'streamId is required'
    streamId = cfg.streamId
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
            streamRevision: cfg.streamRevision
            streamId: streamId
            events: uncommitted
        commitStream.write createCommit(cfg)
        commitStream.end()
    commitStream


