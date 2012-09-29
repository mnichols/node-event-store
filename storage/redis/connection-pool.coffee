net = require 'net'
poolMod = require 'generic-pool'

module.exports = (cfg) ->
    pool = poolMod.Pool
        name: 'redis'
        create: (cb) ->
            try
                conn = net.createConnection cfg.port, cfg.host
                return cb null, conn
            catch err
                cb err
        destroy: (conn) ->
            conn.end()
        max: cfg.maxConnections
        idleTimeoutMillis: 30000
        log: false

    pool.createConnection = ->
    pool


