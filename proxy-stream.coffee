{Stream} = require 'stream'
es = require 'event-stream'
module.exports = ->
    proxy = new Stream()
    buffer = []
    proxy.writable = true
    proxy.readable = true
    src = null
    proxied = null
    ended = false
    lazy = false
    drain = null

    proxy.on 'pipe', (pipeSrc) ->
        src = pipeSrc

            
    proxy.pipe = (pipeDest) ->
        if proxied
            src.removeAllListeners()
            src.pipe(proxied).pipe(pipeDest)
        else
            drain = (proxied) ->
                proxy.pipe(proxied).pipe(pipeDest)
                for d in buffer
                    proxy.emit 'data', d

    proxy.enable = (target) ->
        console.log 'enable'
        return if proxied
        proxied = target

    proxy.write = (data) -> buffer.push data
    proxy.end = -> 
        console.log 'proxyend'
        return if ended
        ended = true
        drain proxied if drain
        proxy.emit 'end'
        proxy.emit 'close'
    proxy





