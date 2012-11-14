es = require 'event-stream'
{StringDecoder} = require 'string_decoder'
util = require 'util'
sd = new StringDecoder 'utf8'
Transform = require './node_modules/readable-stream/transform'

module.exports = ReplyParser = (stream, cb = ->) ->
    Transform.apply @, [{lowWaterMark: 0, highWaterMark:0}]
    @done = false
    @replyCount = 1
    if 'function' == typeof stream
        cb = stream 
        stream = null
    finish = (filter) =>
        @done = (@replyCount<=0)
        cb null
        @emit 'done' if @done
    
    @_transform =  (chunk, outputFn, cb = ->) =>
        multiBulk = null
        replyCount = 1
        bulk = false
        all = sd.write chunk #string reply
        parts = all.split '\r\n'
        next = (err) ->
            return cb err
        
        for reply in parts when reply.length
            first = reply[0]
            content = reply.slice(1)
            replyCount--
            switch reply
                when '*0' #key doesnt exist
                    outputFn new Buffer('')
                    return next()
                when '$-1', '*-1' #value doesnt exist
                    outputFn new Buffer(null)
                    return next()
            switch first
                when '$'
                    bulk = parseInt(content)
                    #next()
                when '+', ':'
                    outputFn new Buffer(content)
                    #next()
                when '-'
                    next new Error content
                # multi-bulk 
                when '*'
                    replyCount = parseInt(content)*2
                    multiBulk = []
                    #next()
                when '~'
                    replyCount++
                    #next()
                else
                    unless bulk
                        throw new Error "bad reply: #{reply}"
                    multiBulk.push reply if multiBulk
                    if replyCount<=0
                        #output = if multiBulk then Buffer.concat(multiBulk) else new Buffer(reply)
                        output = multiBulk or reply
                        console.log 'outputting', output
                        outputFn output
                        return next()

    @

util.inherits ReplyParser, Transform
                #    filter = es.map (reply, next) ->
                #        first = reply[0]
                #        content = reply.slice(1)
                #        @replyCount--
                #        switch reply
                #            when '*0' #key doesnt exist
                #                finish @
                #                return next null, ''
                #            when '$-1', '*-1' #value doesnt exist
                #                finish @
                #                return next null, null
                #        switch first
                #            when '$'
                #                @bulk = parseInt(content)
                #                return next()
                #            when '+', ':'
                #                finish @
                #                return next null, content
                #            when '-'
                #                finish @
                #                return next new Error content
                #            when '*'
                #                @replyCount = parseInt(content)*2
                #                return next()
                #            when '~'
                #                @replyCount++
                #                return next()
                #            else
                #                if @bulk
                #                    finish @
                #                    return next null, reply
                #                throw new Error "bad reply: #{reply}"
