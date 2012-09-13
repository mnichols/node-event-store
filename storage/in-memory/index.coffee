util = require 'util'
{EventEmitter2}= require 'eventemitter2'
es = require 'event-stream'
mapStream = require 'map-stream'

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

        Storage::createReader = (filter, opts={flatten:true}) ->

            reader = es.map (data, next) =>
                key = @getCommitsKey filter.streamId
                commits = @storage[key] ? []
                return next() unless commits.length > 0
                for c in commits when c.streamRevision >= filter.minRevision and c.streamRevision <= filter.maxRevision
                    reader.streamRevision = c.streamRevision
                    for e in c.payload
                        next null, e

                reader.end()

            reader.streamRevision = 0
            reader.read = -> reader.write null
            reader

        Storage::createCommitter = ->

            committer = es.map (commit, next) =>
                key = @getCommitsKey commit.streamId
                @storage[key] = (@storage[key] ? []).concat commit
                next null, commit

        new Storage cfg




