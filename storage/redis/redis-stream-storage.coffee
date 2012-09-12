es = require 'event-stream'
Redis = require 'redis-stream'
{EventEmitter2} = require 'eventemitter2'
util = require 'util'

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
        ConcurrencyError = ->
            Error.apply @, arguments
            @name = 'ConcurrencyError'

        ConcurrencyError:: = new Error()
        ConcurrencyError::constructor = ConcurrencyError
        isNumber = (obj) ->
            toString.call(obj)=='[object Number]'

        slice = Array::slice
        concat = (target=[], data=[]) -> Array::push.apply target, data

        Storage = (cfg) ->
            EventEmitter2.call @
            @id = cfg.id
            process.nextTick => @emit 'storage.ready', @

        util.inherits Storage, EventEmitter2
        Storage::createReader = ->
            reader = es.map (data, next) ->
                next null, data
            
            flatten = es.map (data, next) ->
                return next() unless data?.payload
                events = data.payload.map (e) ->
                    (e[k]=data[k]) for k,v of data when k!='payload'
                    e
                next null, events

            ###
            *Begins stream events
            *@method read
            *@param {Object} filter The definition for the stream
            *    @param {String} streamId The id for the stream
            *    @param {Number} [minRevision=0] The minimum revision to start stream at
            *    @param {Number} [maxRevision=Number.MAX_VALUE] The max revision
            *@param {Object} [opts]
            *    @param {Boolean} [flatten=true] Emit events with commit descriptors
            ###
            reader.read = (filter, opts={flatten:true}) ->
                id = cfg.getCommitsKey(filter.streamId)
                finish = if opts.flatten then flatten else reader
                streams = [
                    client.stream()
                    es.parse()
                    finish
                ]
                pipe = es.pipeline.apply @, streams
                #proxy stream commands to our pipe
                reader.pause = pipe.pause
                reader.resume = pipe.resume
                #proxy events from pipe to reader
                pipe.emit = -> reader.emit.apply reader, arguments
                #we have to pass an arg in to the underlying redis-stream
                pipe.write [
                    'zrangebyscore', 
                    id, 
                    filter.minRevision, 
                    filter.maxRevision
                ]
                pipe.end()
            reader
        Storage::createCommitter = ->
            self = @
            emitter = es.map (commit, next) =>
                #delegate our commit event
                emitter.on 'commit', (data) ->
                    self.emit "#{cfg.id}.commit", data

                id = cfg.getCommitsKey(commit.streamId)
                maxRevision = client.stream()
                writer = client.stream()
                validate = es.map (data, next) =>
                    score = Number(data)
                    #first record is likely the actual object
                    #so just drop this data
                    return next() if isNaN score 
                    if score==commit.checkRevision
                        #build redis add command
                        #here to pass into next stream
                        writeCmd = [
                            'zadd'
                            id
                            commit.streamRevision
                            JSON.stringify(commit)
                        ]
                        return next null, writeCmd
                    next new ConcurrencyError()
                done = es.map (data, next) =>
                    delete commit.checkRevision
                    emitter.emit 'commit', commit
                    emitter.emit 'data', commit #for piping
                cmd = es.pipeline(
                        maxRevision,
                        validate,
                        writer,                    
                        done
                    )
                cmd.on 'error',  (err) -> emitter.emit 'error', err
                cmd.write ['zrevrange', id, 0, 1, 'WITHSCORES' ]
                cmd.end()

        Storage::read = (filter, callback) ->
            reader = @createReader()
            events = []
            reader.on 'error', => 
                events = []
                callback.apply  @, arguments
            reader.on 'data', (data) =>
                concat events, data
            reader.on 'end', =>
                args = slice.call arguments
                args.unshift events
                args.unshift null
                callback.apply @, args
            reader.read filter

        Storage::write = (commit, callback) ->
            committer = @createCommitter()
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



