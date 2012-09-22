es = require 'event-stream'
CommitStream = require './commit-stream'

createCommit = (headers = []) ->
    cfg = @
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
    headers: headers
    payload: cfg.events

    
module.exports = (source, storage, cfg = {}) ->
    source.createCommit = ->
        unless source.committable
            throw new Error 'source is not committable. this is likely due to not having been read from storage.'
        commit = createCommit.apply source, []
        (commit[k]=cfg[k]) for k,v of commit when ~k.indexOf 'headers'
        commit
    unless source.streamId
        throw new Error 'streamId is required'
    streamable = new CommitStream source
    streamable.pipe(storage.commitStream())
    streamable
