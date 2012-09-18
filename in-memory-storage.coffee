util = require 'util'
{EventEmitter2}= require 'eventemitter2'
es = require 'event-stream'

defaultCfg =
    id: 'in-memory'
    getCommitsKey: (streamId) -> "commits:#{streamId}"

module.exports = 
    createStorage: (cfg = {}) ->
        (cfg[k]=defaultCfg[k]) for k, v of defaultCfg

        Storage = (cfg) ->
            EventEmitter2.call @
            (@[k]=cfg[k]) for k,v of cfg
            @storage = {}

            process.nextTick => @emit 'storage.ready', @

        util.inherits Storage, EventEmitter2
        Storage::mount = (hash = {}) ->
            (@storage[k]=hash[k]) for k,v of hash

        Storage::purge = ->
            @storage = {}

        Storage::createReader = (filter, opts = {}) ->
            defaultOpts = 
                flatten: true
                enrich: false
            (opts[k]=defaultOpts[k]) for k,v of defaultOpts

            key = @getCommitsKey filter.streamId
            commits = @storage[key] ? []
            high = 0
            events = []
            for c in commits when c.streamRevision >= filter.minRevision and c.streamRevision <= filter.maxRevision
                high = c.streamRevision
                if opts.flatten
                    events = events.concat c.payload
                else
                    events.push c.payload

            reader = es.readArray events
            reader.streamRevision = high
            reader

        Storage::commitStream = ->

            committer = es.map (commit, next) =>
                key = @getCommitsKey commit.streamId
                @storage[key] = (@storage[key] ? []).concat commit
                next null, commit

        new Storage cfg




