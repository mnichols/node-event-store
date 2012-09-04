es = require 'event-stream'
Redis = require 'redis-stream'
{EventEmitter} = require 'events'
util = require 'util'

getCommitsKey =  (streamId) -> "commits:#{streamId}"
module.exports = 
    createStorage: (client, cb) ->
        ConcurrencyError = ->
            Error.apply @, arguments
            @name = 'ConcurrencyError'

        ConcurrencyError:: = new Error()
        ConcurrencyError::constructor = ConcurrencyError
        isNumber = (obj) ->
            toString.call(obj)=='[object Number]'

        slice = Array::slice
        concat = (target=[], data=[]) -> Array::push.apply target, data

        Storage = ->
            EventEmitter.call @
            util.inherits Storage, EventEmitter
            createReader: ->
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
                *    @param {Boolean} [flatten=true] Emit events one at a time from 
                *        underlying payload of each commit
                ###
                reader.read = (filter, opts={flatten:true}) ->
                    id = getCommitsKey(filter.streamId)
                    finish = if opts.flatten then flatten else reader
                    streams = [
                        client.stream('zrangebyscore', id, filter.minRevision)
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
                    pipe.write filter.maxRevision
                    pipe.end()
                reader

            createCommitter: ->
                emitter = es.map (commit, next) ->
                    id = getCommitsKey(commit.streamId)
                    writer = client.stream('zadd',id,commit.streamRevision)
                    maxRevision = client.stream('zrevrange',id,0,1)
                    validate = es.map (data, next) ->
                        score = Number(data)
                        #first record is likely the actual object
                        #so just drop this data
                        return next() if isNaN score 
                        if score==commit.checkRevision
                            return next null, null
                        next new ConcurrencyError()
                    _commit = (data, next) ->
                        delete commit.checkRevision
                        emitter.emit 'commit', commit
                        emitter.emit 'data', commit #for piping
                    _write = ->
                        pipe = es.pipeline(writer, es.map(_commit))
                        pipe.write JSON.stringify commit
                        pipe.end()
                    _error = (err) -> 
                        validate.removeListener 'end', _write
                        validate.destroy()
                        emitter.emit 'error', err

                    validate.on 'error',  _error
                    maxRevision.on 'error', _error
                    writer.on 'error', _error
                    validate.on 'end', _write
                    ck = es.pipeline(
                        maxRevision,
                        validate)
                    ck.write 'WITHSCORES'
                    ck.end()
                    

            read: (filter, callback) ->
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

            write: (commit, callback) ->
                committer = @createCommitter()
                committer.on 'commit', =>
                    args = slice.call arguments
                    args.unshift null
                    callback.apply @, args
                committer.on 'error', (err) =>
                    callback.apply @, arguments
                return committer.write commit
        cb null, new Storage()

