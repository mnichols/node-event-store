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

    addEvents = (events = []) ->
        uncommitted = uncommitted.concat events

    streamableCommit = es.map (events = [], next) ->
        commitStream = storage.createCommitter()
        addEvents events if events
        cfg =
            streamRevision: cfg.streamRevision
            streamId: streamId
            events: uncommitted
        commit = createCommit(cfg)
        commitStream.on 'error', next
        streamableCommit.on 'end', ->
            commitStream.end()
        commitStream.pipe es.map (data, ignore) ->
            next null, data
            ignore()
        commitStream.write commit

    streamableCommit
