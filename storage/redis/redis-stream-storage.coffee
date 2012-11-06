es = require 'event-stream'
{Stream} = require 'stream'
{EventEmitter2} = require 'eventemitter2'
util = require 'util'
Duplex = require './node_modules/readable-stream/duplex'

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

        _createReader = (opts={}) ->
            defaultOpts = 
                enrich: false
                flatten: true
                emitStreamHeader: false
            (opts[k]=defaultOpts[k]) for k,v of defaultOpts when !opts[k]
            args = (cmd, filter) -> 
                id = cfg.getCommitsKey(filter.streamId)
                arr = [id, filter.minRevision, filter.maxRevision]
                arr.unshift cmd
                arr
            enrich = (data) ->
                events = data.payload.map (e) ->
                    (e[k]=data[k]) for k,v of data when k!='payload'
                    e
                return events
            countStream = cfg.client.stream()
            rangeStream = cfg.client.stream()
            class Reader extends Duplex
                _write: (filter, cb) ->

                _read: (n, cb) ->
                    #            Reader = (opts) ->
                    #                Duplex.apply @,arguments
                    #            Reader::_write = (filter, cb) ->
                    #                console.log '_write', filter
                    #            Reader::_read = (n, cb) ->
                    #                console.log '_read'
                    #                cb null, 'data'
            new Reader opts

        ###
        * reader implementation for Redis
        * this streams events out of Redis storage
        * @params {Object} [opts]
        *     @params {Boolean} [enrich=false] Whether to add details of the commit onto each event
        *     @params {Boolean} [flatten=true] Whether to emit events in groups by commit, or singly
        *     @params {Boolean} [emitStreamHeader=false] Whether to emit a single 'data' event before streaming committed event data
        ###
        _createReader2 = (opts = {}) ->
            defaultOpts = 
                enrich: false
                flatten: true
                emitStreamHeader: false
            (opts[k]=defaultOpts[k]) for k,v of defaultOpts when !opts[k]
            args = (cmd, filter) -> 
                id = cfg.getCommitsKey(filter.streamId)
                arr = [id, filter.minRevision, filter.maxRevision]
                arr.unshift cmd
                arr
            enrich = (data) ->
                events = data.payload.map (e) ->
                    (e[k]=data[k]) for k,v of data when k!='payload'
                    e
                return events
            countStream = cfg.client.stream()
            rangeStream = cfg.client.stream()

            counter = (stream, filter) ->
                copy = {}
                (copy[k]=filter[k]) for k,v of filter
                xformCount = es.map (data, next) ->
                    next null, args('zcount', copy)

                record = es.map (commitCount, next) ->
                    stream.commitCount = parseInt(commitCount)
                    next null, parseInt(commitCount)
                es.pipeline xformCount, countStream, record

            header = (stream, filter) ->
                copy = {}
                (copy[k]=filter[k]) for k,v of filter
                countStream.once 'data', (commitCount) ->
                    commitCount = parseInt(commitCount)
                    if opts.emitStreamHeader
                        header = {}
                        (header[k] = filter[k]) for k,v of copy
                        header.commitCount = commitCount
                        stream.emit 'data', header
            ranger = (stream, filter) ->
                copy = {}
                (copy[k]=filter[k]) for k,v of filter
                xformRange = es.map (commitCount, next) ->
                    next null, args('zrangebyscore', copy)
                es.pipeline xformRange, rangeStream, es.parse()

            payload = (stream) ->
                es.map (data, next) =>
                    #update the stream's revision
                    stream.streamRevision = data.streamRevision
                    return next null, data.payload unless opts.enrich
                    next null, enrich(data)
            each = (stream) -> 
                es.through (events) ->
                    #buffer if paused
                    if opts.flatten
                        for e in events
                            stream.emit 'data', e
                    else
                        stream.emit 'data', events    
            done = (stream, cb = ->) ->
                es.through (events) ->
                    stream.inputs++
                    console.log 'inputs', stream.inputs
                    if stream.inputs>=stream.commitCount
                        stream.emit 'done', stream.commitCount 
                        cb null, events

            ###
            * @params {Object} filter
            *     @params {String} streamId The id (typically of aggregate root) of the stream
            *     @params {Number} [minRevision=0] The stream revision to start at
            *     @params {Number} [maxRevision=Number.MAX_VALUE] The stream revision to end with
            ###
            inner = es.through (filter) ->

            stream = es.through (filter) ->
                unless filter
                    throw new Error 'filter is required'
                inner.queue filter
                stream.pause()
                inner.resume()
                
            inner.on 'data', (filter) ->
                stream.streamRevision = 0
                stream.inputs = 0
                countStream.removeAllListeners 'pipe'
                rangeStream.removeAllListeners 'pipe'
                header(stream, filter)
                resume = done stream, ->
                    stream.resume()

                pipe = es.pipeline counter(stream, filter),
                    ranger(stream, filter),
                    payload(stream),
                    each(stream),
                    resume
                pipe.write filter

            
            stream


        Storage::createReadable = (opts) ->
            reader = _createReader opts
            reader
        Storage::createReader = (filter, opts) ->
            reader = @createReadable opts
            console.log 'starting reader'
            process.nextTick -> reader.write filter
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

            stream = es.pipeline concurrency,
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



