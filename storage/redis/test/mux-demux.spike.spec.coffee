es = require 'event-stream'
net = require 'net'
MuxDemux = require 'mux-demux'
Redis = require '../redis-stream'
describe 'muxer', ->

    describe 'reader', ->

        it 'should stream', (done) ->
            #mdm1 = MuxDemux()
            #raw = es.through (reply) ->
            #    @emit 'raw data', reply

            #conn = net.createConnection 6379, 'localhost'
            #ds = mdm1.createWriteStream('reply')
            #stream = es.through (args) ->
            #    parsed = Redis.parse args
            #    conn.pipe(es.split('\r\n'))
            #        .pipe(interpret)
            #        .pipe(mdm1)
            #    conn.write parsed

            #interpret = es.through (data) ->
            #    console.log 'data interpret', data
            #    ds.write data

            #stream.pipe(mdm1).pipe(raw)
            #stream.write ['select', 11]

                
                

                



            mdm1 = MuxDemux()
            mainData = [0..20]
            reader = es.readable (cnt, callback) ->
                if cnt == mainData.length
                    return @emit 'end'

                callback null, mainData[cnt]

            writer = es.through (data) ->
                console.log "got #{data}"

            dupe= es.duplex writer, reader
            ds = mdm1.createWriteStream 'reply'
            thru = es.through (data) ->
                ds.write data*2

            reader.on 'end', -> done()
            dupe.pipe(thru).pipe(mdm1).pipe(dupe)

        
        it 'should be happy', (done) ->
            mdm1 = MuxDemux()
            ds = mdm1.createWriteStream 'reply'

            interpret = es.through (data) ->
                #if reply starts with an '+', strip and send remainder then end
                #if reply starts with an ':', strip ':' then pass thru and end
                #if reply starts with an '*', start counter based on number; ie *3 would receive 3 replies before 'end'
                #if reply starts with an '-', then error
                #if reply starts with '$' and 'counter' is 0, then strip $ and send on then end
                #SPECIAL if reply is $-1 or *-1 then pass NULL and end
                #SPECIAL if reply is *0 then pass empty list ''
                console.log 'data interpret', data
                @emit 'data', data.toString()
            getConn = (cb) ->
                conn = net.createConnection 6379, 'localhost'
                cb null, conn
            main = es.through (args) ->
                console.log 'received main', args
                parsed = Redis.parse args
                behalf = es.through (reply) ->
                    console.log 'onbehalf', reply
                    main.emit 'data', reply
                getConn (err, conn) ->
                    conn.pipe(interpret).pipe(behalf)
                    conn.write parsed

            ck = es.through (data) ->
                data.should.equal '+OK\r\n'
                done()
                
            main.pipe(ck)
            main.write ['select', 11]
        it 'should support tcp', (done) ->
            mdm1 = MuxDemux()
            mainData = [0..2]
            parsed = Redis.parse ['select', 11]
            ds = mdm1.createWriteStream 'reply'
            main = es.through (data) ->
                console.log 'maindata', data
            interpret = es.through (data) ->
                console.log 'data interpret', data
                ds.write data.toString()
            thru = es.through (data) ->
                conn.write parsed
            conn.pipe(interpret).pipe(mdm1).pipe(main)
            conn.write(parsed)

            return 
            reader = es.readable (cnt, callback) ->
                if cnt == mainData.length
                    return @emit 'end'

                callback null, mainData[cnt]

            writer = es.through (data) ->
                console.log "got #{data}"

            dupe= es.duplex writer, reader
            thru = es.through (data) ->
                conn.write parsed


            reader.on 'end', -> done()


                



