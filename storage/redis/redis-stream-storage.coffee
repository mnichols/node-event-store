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

        slice = Array::slice
        concat = (target=[], data=[]) -> 
            data = [data] unless Array.isArray data
            Array::push.apply target, data

        Storage = (cfg) ->
            EventEmitter2.call @
            @id = cfg.id
            process.nextTick => @emit 'storage.ready', @

        util.inherits Storage, EventEmitter2
        Storage::createReader = (filter, opts = {}) ->
            defaultOpts = 
                enrich:false
                flatten: true
            (opts[k]=defaultOpts[k]) for k,v of defaultOpts when !opts[k]
            id = cfg.getCommitsKey(filter.streamId)
            args = (cmd) -> 
                arr = [id, filter.minRevision, filter.maxRevision]
                arr.unshift cmd
                arr
            inputs = 0
            ended = false
            paused = false
            tryFlush = -> throw new Error('flushing has not been enabled yet')
            stream = new Stream()
            stream.readable = true
            stream.writable = false
            stream.streamRevision = 0

            stream.pause = ->
                paused = true

            stream.resume = ->
                paused = false
                tryFlush()

            stream.end = ->
                return if ended
                ended = true
                stream.emit 'end'
                stream.emit 'close'

            stream.destroy = ->
                return if ended
                ended = true
                stream.emit 'end'
                stream.emit 'close'
            
            enrich = (data) ->
                events = data.payload.map (e) ->
                    (e[k]=data[k]) for k,v of data when k!='payload'
                    e
                return events

            payload =
                es.map (data, next) =>
                    return next() unless data.payload
                    #update the stream's revision
                    stream.streamRevision = data.streamRevision
                    return next null, data.payload unless opts.enrich
                    next null, enrich(data)

            each = es.through (events) ->
                inputs++
                #buffer if paused
                if opts.flatten
                    for e in events
                        stream.emit 'data', e
                else
                    stream.emit 'data', events    
                tryFlush()

            flusher = (commitCount) ->
                ->
                    #todo need to buffer during pause 
                    #and then emit data events on resume
                    return if paused or inputs < commitCount
                    stream.end()
        
            _read = (commitCount) ->
                console.log 'redis-storage',"streaming #{commitCount} commits"
                tryFlush = flusher commitCount
                rangeStream = cfg.client.stream()
                rangeStream.pipe(es.parse()).pipe(payload).pipe(each)
                rangeStream.write args('zrangebyscore')


            _begin = ->
                countStream = cfg.client.stream()
                countStream.on 'data', (data) ->
                    commitCount = Number(data)
                    if commitCount==0
                        console.log 'redis-storage',
                            "new stream detected for stream '#{filter.streamId}'"
                        countStream.end()
                        return stream.end()
                    _read(commitCount)
                countStream.write args('zcount')

            process.nextTick _begin
            return stream

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
            reader = @createReader filter,
                enrich: false
                flatten: true
            buf = es.writeArray callback
            reader.pipe buf

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



