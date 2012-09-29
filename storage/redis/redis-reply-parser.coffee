es = require 'event-stream'

module.exports = (stream, cb = ->) ->
    done = (filter) ->
        filter.done = (filter.replyCount==0)
        cb null
        filter.emit 'done' if filter.done

    passes = -1
    filter = es.map (reply, next) ->
        passes++
        #first pass is DB selection
        return unless passes
        first = reply[0]
        content = reply.slice(1)
        filter.replyCount--
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
                filter.replyCount = parseInt(content)
                return next()
            when '~'
                filter.replyCount++
                return next null, content
            else
                if filter.bulk
                    done filter
                    return next null, reply
                throw new Error "bad reply: #{reply}"

    filter.replyCount = 1
    filter.done = false

    filter
