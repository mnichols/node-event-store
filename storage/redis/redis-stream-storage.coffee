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
        ###
        * reader implementation for Redis
        * this streams events out of Redis storage
        * @params {Object} filter
        *     @params {String} streamId The id (typically of aggregate root) of the stream
        *     @params {Number} [minRevision=0] The stream revision to start at
        *     @params {Number} [maxRevision=Number.MAX_VALUE] The stream revision to end with
        * @params {Object} [opts]
        *     @params {Boolean} [enrich=false] Whether to add details of the commit onto each event
        *     @params {Boolean} [flatten=true] Whether to emit events in groups by commit, or singly
        *     @params {Boolean} [emitStreamHeader=false] Whether to emit a single 'data' event before streaming committed event data
        ###
        Storage::createReader = (filter, opts = {}) ->
            defaultOpts = 
                enrich:false
                flatten: true
                emitStreamHeader: false
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
        
            _read = (commitCount) =>
                console.log @id,"streaming #{commitCount} commits"
                tryFlush = flusher commitCount
                rangeStream = cfg.client.stream()
                rangeStream.pipe(es.parse()).pipe(payload).pipe(each)
                rangeStream.write args('zrangebyscore')

            _noCommits = (countStream) ->
                console.log @id,
                    "new stream detected for stream '#{filter.streamId}'"
                if opts.emitStreamHeader
                    header = {}
                    (header[k] = filter[k]) for k,v of filter
                    header.commitCount = 0
                    header.streamRevision = 0
                    stream.emit 'data', header

                countStream.end()
                return stream.end()

            _begin = =>
                countStream = cfg.client.stream()
                countStream.on 'data', (data) =>
                    commitCount = Number(data)
                    return _noCommits(countStream) if commitCount == 0
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
            stream = es.map (commit, next) =>
                concurrency = concurrencyStream commit, cfg
                _write = ->
                _end = =>
                    stream.emit 'commit', commit
                    @emit "#{cfg.id}.commit", commit
                    next null, commit
                thru = es.through _write, _end
                concurrency.on 'error', (err) -> 
                    next err
                concurrency.pipe(xformWriteArgs)
                    .pipe(writer)
                    .pipe(es.through(_write, _end))

            return stream

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



