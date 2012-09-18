es = require 'event-stream'
{Stream} = require 'stream'
Redis = require 'redis-stream'
{EventEmitter2} = require 'eventemitter2'
util = require 'util'

concurrencyStream = require './concurrency-stream'

defaultCfg = 
    id: 'redis-storage'
    client: null
    getCommitsKey: (streamId) -> "commits:#{streamId}"

module.exports = 
    createStorage: (cfg = defaultCfg, cb) ->
        (cfg[k]=defaultCfg[k]) for k,v of defaultCfg when !cfg[k]
        client = cfg.client
        unless client?
            throw new "A redis client is required"
        ConcurrencyError = (message) ->
            Error.apply @, arguments
            @name = 'ConcurrencyError'
            @message = message ? 'ConcurrencyError'

        ConcurrencyError:: = new Error()
        ConcurrencyError::constructor = ConcurrencyError

        isArray = (obj) ->
            Object::toString.call obj == '[object Array]'
        slice = Array::slice
        concat = (target=[], data=[]) -> 
            data = [data] unless isArray data
            Array::push.apply target, data

        Storage = (cfg) ->
            EventEmitter2.call @
            @id = cfg.id
            process.nextTick => @emit 'storage.ready', @

        util.inherits Storage, EventEmitter2
        Storage::createReader = (filter, opts={flatten:true}) ->
            id = cfg.getCommitsKey(filter.streamId)
            args = [id, filter.minRevision, filter.maxRevision]
            reader = null
            commitCount = 0
            countStream = cfg.client.stream 'zcount'
            rangeStream = cfg.client.stream 'zrangebyscore'
            countercept =
                es.map (data, next) ->
                    commitCount = Number(data)
                    console.log 'redis-storage',"streaming #{commitCount} commits"
                    if commitCount == 0
                        console.log 'redis-storage',
                            "new stream detected for stream '#{filter.streamId}'"
                        reader.end()
                    next null, args
            
            flatten = (data) ->
                events = data.payload.map (e) ->
                    (e[k]=data[k]) for k,v of data when k!='payload'
                    e
                return events

            payload =
                es.map (data, next) =>
                    return next() unless data.payload
                    #update the stream's revision
                    reader.streamRevision = data.streamRevision
                    events = if opts.flatten then flatten(data) else data.payload
                    next null, events
            eachEvent = (require './each-event-stream')()
            
            eachEvent.on 'tick', (inputs) =>
                reader.end() if inputs>=commitCount

            reader = es.pipeline(
                countStream,
                countercept,
                rangeStream,
                es.parse(),
                payload,
                eachEvent
            )
        
            reader.streamRevision = 0 #initialize revision
            write = reader.write
            reader.read = -> write args
            reader.write = -> 
                throw new Error('event storage readers are readable only. prefer `read()`')

            return reader

        Storage::commitStream = ->
            xformWriteArgs = es.map (commit, next) ->
                #no need to store this check
                delete commit.checkRevision
                args = [
                    'zadd'
                    cfg.getCommitsKey(commit.streamId)
                    commit.streamRevision
                    JSON.stringify(commit)
                ]
                next null, args
            writer = cfg.client.stream()
            stream = new Stream()
            ended = false
            destroyed = false
            stream.writable = stream.readable= true
            stream.write = (commit) ->
                concurrency = concurrencyStream commit, cfg
                writer.on 'end', -> 
                    stream.emit 'data', commit
                    stream.end()
                concurrency.on 'error', (err) -> stream.emit 'error', err
                concurrency.pipe(xformWriteArgs).pipe(writer)

            stream.end = ->
                return if ended
                ended = true
                stream.emit 'end'
                stream.emit 'close'

            stream.destroy = ->
                stream.emit 'end'
                stream.emit 'close'
                ended = true

            stream.on 'data', (commit) =>
                stream.emit 'commit', commit
                @emit "#{cfg.id}.commit", commit

            stream

        Storage::read = (filter, callback) ->
            reader = @createReader filter
            events = []
            reader.on 'error', => 
                events = []
                callback.apply  @, arguments
            reader.on 'data', (data) =>
                events.push data
            reader.on 'end', =>
                callback null, events
            reader.read()

        Storage::write = (commit, callback) ->
            committer = @commitStream()
            committer.on 'commit', =>
                args = slice.call arguments
                args.unshift null
                callback.apply @, args
            committer.on 'error', (err) =>
                callback.apply @, arguments
            return committer.write commit
        storage = new Storage cfg        
        cb null, storage if cb?
        storage



