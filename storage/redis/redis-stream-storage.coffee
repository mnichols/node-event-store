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
        ConcurrencyError = (message) ->
            Error.apply @, arguments
            @name = 'ConcurrencyError'
            @message = message ? 'ConcurrencyError'

        ConcurrencyError:: = new Error()
        ConcurrencyError::constructor = ConcurrencyError
        isNumber = (obj) ->
            toString.call(obj)=='[object Number]'

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
            reader.read = -> reader.write args

            return reader
        Storage::createCommitter = ->
            buildValidator = (commit) =>
                (data, next) ->                
                    score = Number(data)
                    #first record is likely the actual object
                    #so just drop this data
                    return next() if isNaN score 
                    if score==commit.checkRevision
                        args = [
                            'zadd'
                            cfg.getCommitsKey(commit.streamId)
                            commit.streamRevision
                            JSON.stringify(commit)
                        ]
                        return next null, args
                    err = new ConcurrencyError "Expected #{commit.checkRevision}, but got #{score}"

                    next err
            buildDone = (commit) =>
                (data, next) ->
                    delete commit.checkRevision
                    next null, commit

            validator = -> throw new Error('"validator" not implemented')
            done = -> throw new Error('"done" not implemented')
            maxRevision = client.stream()
            validate = es.map (data, next) =>
                validator data, next
            writer = client.stream()
            finisher = es.map (data, next) =>
                done data, next
                pipe.end()
            pipe = es.pipeline(
                maxRevision,
                validate,
                writer,
                finisher
                )

            pipe.on 'data', (data) => pipe.emit 'commit', data
            pipe.on 'commit', (data) => @emit "#{cfg.id}.commit", data

            originalWrite = pipe.write
            pipe.write = (commit) =>
                unless commit
                    throw new Error 'commit object is required'
                id = cfg.getCommitsKey commit.streamId
                validator = buildValidator commit
                done = buildDone commit
                originalWrite ['zrevrange', id, 0, 1, 'WITHSCORES' ]

            return pipe

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



