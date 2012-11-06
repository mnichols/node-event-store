es = require 'event-stream'
Transform = require './node_modules/readable-stream/transform'

module.exports = (stream, cb = ->) ->
    if 'function' == typeof stream
        cb = stream 
        stream = null
    done = (filter) ->
        filter.done = (filter.replyCount<=0)
        cb null
        filter.emit 'done' if filter.done
    class Filter extends Transform
        constructor: (@replyCount=1, @done=false) ->
        _transform: (reply, outputFn, next) ->
            first = reply[0]
            content = reply.slice(1)
            @replyCount--
            switch reply
                when '*0' #key doesnt exist
                    done filter
                    outputFn ''
                    return next()
                when '$-1', '*-1' #value doesnt exist
                    done filter
                    outputFn null
                    return next()
            switch first
                when '$'
                    filter.bulk = parseInt(content)
                    return next()
                when '+', ':'
                    done filter
                    outputFn content
                    return next()
                when '-'
                    done filter
                    return next new Error content
                when '*'
                    filter.replyCount = parseInt(content)*2
                    return next()
                when '~'
                    filter.replyCount++
                    return next()
                else
                    if filter.bulk
                        done filter
                        outputFn reply
                        return next()
                    throw new Error "bad reply: #{reply}"


    filter = es.map (reply, next) ->
        first = reply[0]
        content = reply.slice(1)
        filter.replyCount--
        switch reply
            when '*0' #key doesnt exist
                done filter
                return next null, ''
            when '$-1', '*-1' #value doesnt exist
                done filter
                return next null, null
        switch first
            when '$'
                filter.bulk = parseInt(content)
                return next()
            when '+', ':'
                done filter
                return next null, content
            when '-'
                done filter
                return next new Error content
            when '*'
                filter.replyCount = parseInt(content)*2
                return next()
            when '~'
                filter.replyCount++
                return next()
            else
                if filter.bulk
                    done filter
                    return next null, reply
                throw new Error "bad reply: #{reply}"

    filter.replyCount = 1
    filter.done = false

    filter
