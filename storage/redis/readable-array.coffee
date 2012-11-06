Readable = require './node_modules/readable-stream'
util = require 'util'
module.exports = ReadableArray = (arr) ->
    Readable.apply @
    @arr = arr.slice()

    @_read = (n, cb) ->
        return cb() if @arr.length==0
        data = @arr.shift()
        cb null, new Buffer(data)
    @

util.inherits ReadableArray, Readable

