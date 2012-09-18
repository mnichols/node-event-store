es = require 'event-stream'
CommitStream = require './commit-stream'

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
    streamRevision: (cfg.streamRevision ? 0) + (cfg?.events?.length ? 0)
    timestamp: utc
    headers: []
    payload: cfg.events

    
module.exports = (cfg, storage) ->
    unless cfg.streamId
        throw new Error 'streamId is required'
    cfg =
        streamRevision: cfg.streamRevision
        streamId: cfg.streamId
    commit = createCommit(cfg)
    streamable = new CommitStream commit
    streamable.on 'committable', (commit) ->
        storage.writeCommit commit
    streamable
