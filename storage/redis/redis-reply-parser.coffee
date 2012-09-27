es = require 'event-stream'

module.exports = (stream) ->
    filter = es.map (reply, next) ->
        first = reply[0]
        content = reply.slice(1)
        switch first
            when '$'
                filter.bulk = parseInt(content)
                return next()
            when '+', ':'
                filter.done = true
                return next null, content
            when '-'
                filter.done = true
                return next new Error content
            when '*'
                filter.multi = parseInt(content)
                return next()
            else
                if filter.bulk
                    filter.done = !(--filter.multi)
                    return next null, reply
                throw new Error "bad reply: #{reply}"

    filter.multi = 1
    filter
