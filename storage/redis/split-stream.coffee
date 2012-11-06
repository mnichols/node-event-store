Transform = require './node_modules/readable-stream/transform'
util = require 'util'
{StringDecoder} = require 'string_decoder'
sd = new StringDecoder 'utf8'
module.exports = SplitStream = (delim='\r\n') ->
    Transform.apply @, [{lowWaterMark: 0, highWaterMark:0}]
    @_transform = (chunk, outputFn, cb) ->
        str = sd.write(chunk)
        bufs= []
        arr =  str.split(delim).map (r) ->
            new Buffer r
        for a in arr
            outputFn a
        cb()

    @
util.inherits SplitStream, Transform


