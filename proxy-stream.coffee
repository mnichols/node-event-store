{Stream} = require 'stream'
es = require 'event-stream'
module.exports = ->
    proxy = new Stream()
    proxy.writable = true
    proxy.readable = true
    src = null
    dest = null
    proxied = null

    proxy.on 'pipe', (pipeSrc) ->
        src = pipeSrc
    proxy.pipe = (pipeDest) ->
        unless proxied
            throw new Error 'proxied stream has not been enabled'
        src.removeAllListeners()
        src.pipe(proxied).pipe(pipeDest)
    proxy.enable = (target) ->
        return if proxied
        proxied = target
    proxy





