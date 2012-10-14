net = require 'net'
poolMod = require 'generic-pool'
es = require 'event-stream'
{Stream} = require 'stream'

module.exports = (cfg) ->
    id = 0
    pool = null
    factory = 
        name: 'redis'
        create: (cb=->) ->
            try
                conn = net.createConnection cfg.port, cfg.host
                conn.id = id++
                return cb null, conn
            catch err
                console.error err
                cb err
        destroy: (conn) ->
            conn.end()
        max: cfg.maxConnections
        min: 2
        idleTimeoutMillis: 10000
        log: false
        createConnection: ->
            conn = net.createConnection cfg.port, cfg.host
        numberOfConnectionsUsed: 0
    pool = poolMod.Pool factory


    pool.createProxy = ->

        buffer = []
        ended = false
        destroyed = false
        proxy = new Stream()
        proxy.readable = true
        proxy.writable = true
        proxy.paused = false
        proxy.buffer = buffer



        proxy.on 'end', ->
            proxy.readable = false
            return if proxy.writable
            process.nextTick ->
                proxy.destroy()

        _end = ->
            proxy.writable = false
            if proxy.connection
                proxy.connection.end.call proxy.connection

        proxy.end = ->
            return if ended
            ended = true
            proxy.writable = false
            proxy.write.call proxy if arguments.length
            proxy.release ->
                process.nextTick ->
                    proxy.emit 'end'
            return if proxy.readable
            proxy.destroy()
            

        proxy.destroy = ->
            return if destroyed
            destroyed = true
            ended = true
            buffer.length = 0
            proxy.writable = proxy.readable = false
            proxy.emit 'close'
        proxy.pause = ->
            proxy.paused = true
        proxy.resume = ->
            _drain = proxy.paused
            proxy.paused = false
            proxy.emit 'drain' if _drain

        proxy.write = (data) ->
            unless proxy.writer
                throw new Error 'proxy `connect` has not been called' 
            resumeable = proxy.writer.write data
            unless resumeable
                proxy.writer.once 'drain', ->
                    proxy.resume()
            resumeable

        proxyEvent = (event) ->
            -> 
                args = Array::slice.call arguments
                args.unshift event
                emit.apply proxy, args
        proxy.release = (cb = ->) ->
            return cb() unless proxy.writer
            _release = ->
                if proxy.connection
                    pool.release proxy.connection
                proxy.busy = false
                proxy.connection = null
                proxy.writer = null
                cb()
            
            if proxy.busy
                return proxy.once 'releasable', _release
            _release()

            

        proxy.connect = (cb = ->) ->
            proxy.pause()
            pool.acquire (err, conn) ->

                console.error err if err?
                return cb err if err?
                conn.removeAllListeners 'pipe'
                before = es.map (cmd, next) ->
                    proxy.busy = true
                    next null, cmd
                    
                after = es.through (reply) ->
                    proxy.busy = false
                    proxy.emit 'data', reply
                    proxy.emit 'releasable'

                proxy.connection = conn
                proxy.busy = false
                proxy.writer = es.pipeline before, conn, 
                    es.pipeline after
                cb null, conn
                proxy.resume()
        proxy

    pool
