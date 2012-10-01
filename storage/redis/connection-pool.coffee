net = require 'net'
poolMod = require 'generic-pool'
es = require 'event-stream'
{Stream} = require 'stream'

module.exports = (cfg) ->
    id = 0
    pool = poolMod.Pool
        name: 'redis'
        create: (cb) ->
            try
                conn = net.createConnection cfg.port, cfg.host
                conn.id = id++
                return cb null, conn
            catch err
                cb err
        destroy: (conn) ->
            conn.end()
        max: cfg.maxConnections
        idleTimeoutMillis: 10000
        log: false

    pool.createProxy = ->
        proxy = es.through (cmd) ->
            return proxy.connection.write(cmd) if proxy.connection
            @pause()
            proxy.connect (err) =>
                proxy.connection.write cmd
                return @resume()

        emit = Stream::emit
        proxyEvent = (event) ->
            -> 
                args = Array::slice.call arguments
                args.unshift event
                emit.apply proxy, args
        proxy.release = ->
            return unless proxy.connection
            pool.release proxy.connection
            #proxy.connection.removeAllListeners('data')
            #proxy.connection.removeAllListeners('end')
            #proxy.connection.removeAllListeners('close')
            proxy.connection = null
            
        proxy.connect = (cb = ->) ->
            proxy.pause()
            pool.acquire (err, conn) ->
                return cb err if err?
                proxy.connection = conn
                unless conn.stamped
                    conn.on 'data', proxyEvent 'data'
                    conn.on 'end', proxyEvent 'end'
                    conn.on 'close', proxyEvent 'close'
                conn.stamped = true
                cb()
                proxy.resume()
        proxy

    pool
