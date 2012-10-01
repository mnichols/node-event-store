es = require 'event-stream'
net = require 'net'
MuxDemux = require 'mux-demux'
describe 'muxer', ->
    describe 'demonstrateerror', (done) ->
        it 'should error once', ->
            errors = 0
            thrower = es.through (data) -> 
                @emit 'error', new Error data
            thru = es.through (data) -> 
                @emit 'data', data
            pipe = es.pipeline thru, thrower
            pipe.on 'error', (err) ->
                errors++
                console.log 'error count', errors
                done() if errors.length > 1
            pipe.write 'meh'


    describe 'reader', ->


        it 'p2p2', (done) ->

            mdm1 = MuxDemux (stream) ->
                stream.on 'data', (data) ->
                    console.log stream.meta, data
            mdm2 = MuxDemux (stream) ->
                stream.on 'data', (data) ->
                    console.log stream.meta, data

            main = es.through (reply) ->
                main.emit 'data', reply
            main.pipe(mdm1).pipe(main)
            mdm2.pipe(main)

            ds1 = mdm2.createStream 'ds1'
            ds2 = mdm2.createStream 'ds2'

            ds1.write 'hello'
            ds2.write 'worls'

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
            parseCommand = require '../redis-command-parser'
            reply = es.through (reply) ->
                @emit 'data', reply+''

            stream = es.through (cmd) ->
                console.log 'inputcmd', cmd
                main = net.createConnection 6379, 'localhost'
                main.pipe(reply)
                main.write parseCommand cmd

            stream.pipe(reply)
                .pipe es.through (reply) ->
                    console.log 'reply2', reply
                    done()
            stream.write ['select', 11]

            #stream has
            #1. input cmd (with xform)
            #2. pass cmd into new connection
            #3. new connection pass its reply into original stream
            #4. carry on


                



