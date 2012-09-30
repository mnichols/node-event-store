es = require 'event-stream'
{Stream} = require 'stream'
{EventEmitter2} = require 'eventemitter2'
util = require 'util'
pipeline = require './pipeline'

concurrencyStream = require './concurrency-stream'

defaultCfg = 
    id: 'redis-storage'
    client: null
    getCommitsKey: (streamId) -> "commits:#{streamId}"

module.exports = 
    createClient: (cfg = {}) ->
        defaultClient = 
            port: 6379
            host: 'localhost'
            db: 0
        (cfg[k]=defaultClient[k]) for k,v of defaultClient when !cfg[k]
        {Redis} = require './redis-streamer'
        return new Redis cfg
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
        _createReader = (streamOpts = {}) ->
            defaultStreamOpts = 
                end: true
            (streamOpts[k]=defaultStreamOpts[k]) for k,v of defaultStreamOpts
            tryFlush = -> throw new Error('flushing has not been enabled yet')
            ended = false
            paused = false
            destroyed = false
            stream = new Stream()
            stream.readable = true
            stream.writable = false

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
                return if destroyed
                destroyed = true
                stream.readable = stream.writable = false
                stream.emit 'end'
                stream.emit 'close'


            args = (cmd, filter) -> 
                id = cfg.getCommitsKey(filter.streamId)
                arr = [id, filter.minRevision, filter.maxRevision]
                arr.unshift cmd
                arr

            rangeStream = cfg.client.stream()
            countStream = cfg.client.stream()
            stream.read = (filter, opts, done = ->) ->
                stream.streamRevision = 0
                defaultOpts = 
                    enrich: false
                    flatten: true
                    emitStreamHeader: false
                (opts[k]=defaultOpts[k]) for k,v of defaultOpts when !opts[k]
                inputs = 0


                enrich = (data) ->
                    events = data.payload.map (e) ->
                        (e[k]=data[k]) for k,v of data when k!='payload'
                        e
                    return events

                payload = (filter, opts) ->
                    es.map (data, next) =>
                        return next() unless data.payload
                        #update the stream's revision
                        stream.streamRevision = data.streamRevision
                        return next null, data.payload unless opts.enrich
                        next null, enrich(data)

                each = (filter, opts) ->
                    es.through (events) ->
                        inputs++
                        #buffer if paused
                        if opts.flatten
                            for e in events
                                stream.emit 'data', e
                        else
                            stream.emit 'data', events    
                        tryFlush()

                flusher = (commitCount, filter, opts) ->
                    ->
                        #todo need to buffer during pause 
                        #and then emit data events on resume
                        return if paused or inputs < commitCount
                        stream.emit 'done', commitCount
                        done null, commitCount
                        stream.end()

                _emitHeader = (commitCount, filter, opts) ->
                    return unless opts.emitStreamHeader
                    header = {}
                    (header[k] = filter[k]) for k,v of filter
                    header.commitCount = commitCount
                    stream.emit 'data', header
                _read = (commitCount, filter, opts) ->
                    #console.log @id,"streaming #{commitCount} commits"
                    tryFlush = flusher commitCount, filter, opts

                    rangeStream.pipe(es.parse())
                        .pipe(payload(filter,opts))
                        .pipe(each(filter, opts))
                    params = args('zrangebyscore', filter)
                    rangeStream.write params

                _noCommits = (filter, opts) ->
                    console.log cfg.id,
                        "new stream detected for stream '#{filter.streamId}'"
                    return stream.end()

                _begin = (filter, opts) ->
                    countCommits = (data) =>
                        commitCount = Number(data)
                        _emitHeader commitCount, filter, opts
                        return _noCommits filter, opts if commitCount == 0
                        _read(commitCount, filter, opts)
                        countStream.end()
                    countStream.once 'data', countCommits
                    params = args('zcount', filter)
                    countStream.write params
                        
                _begin filter, opts

            return stream
        Storage::createReader = (filter, opts = {}) ->
            reader = _createReader()
            if filter
                console.log 'starting reader'
                process.nextTick -> reader.read filter, opts
            reader

        Storage::commitStream = ->
            concurrency = concurrencyStream cfg
            result = null
            xformWriteArgs = es.map (commit, next) ->
                delete commit.checkRevision
                result = commit
                args = [
                    'zadd'
                    cfg.getCommitsKey(commit.streamId)
                    commit.streamRevision
                    JSON.stringify(commit)
                ]
                next null, args

            writer = cfg.client.stream()

            stream = pipeline {rethrow:false},
                    concurrency,
                    xformWriteArgs,
                    writer,
                    es.map (reply, next) =>
                        stream.emit 'commit', result
                        @emit "#{cfg.id}.commit", result
                        next null, result
                
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
                #callback = -> #hack to prevent dupe errors
            return committer.write commit
        storage = new Storage cfg        
        cb null, storage if cb?
        storage



