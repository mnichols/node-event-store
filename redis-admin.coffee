es = require 'event-stream'
Redis = require 'redis-stream'
{EventEmitter2} = require 'eventemitter2'
util = require 'util'
defaultCfg = 
    id: 'redis-storage'
    client: null
    getCommitsKey: (streamId) -> "commits:#{streamId}"

module.exports = 
    createAdmin: (cfg) ->
        Admin = ->
            EventEmitter2.call @
            process.nextTick => @emit 'ready', @
        util.inherits Admin, Eve
        Admin::createRangeStream = (start, end) ->


