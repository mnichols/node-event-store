es = require 'event-stream'
createCommit = (cfg) ->
    console.log 'commitcfg', cfg
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
    commitStream = storage.createCommitter()

    addEvents = (events = []) ->
        uncommitted = uncommitted.concat events

    streamableCommit = es.map (events = [], next) ->
        console.log 'streamable', events
        addEvents events if events
        cfg =
            streamRevision: cfg.streamRevision
            streamId: streamId
            events: uncommitted
        next null, createCommit(cfg)


    pipe = es.pipeline(
        streamableCommit,
        commitStream
    )

    pipe



