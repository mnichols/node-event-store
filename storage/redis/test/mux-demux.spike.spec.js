// Generated by CoffeeScript 1.3.1
(function() {
  var MuxDemux, es, net;

  es = require('event-stream');

  net = require('net');

  MuxDemux = require('mux-demux');

  describe('muxer', function() {
    describe('demonstrateerror', function(done) {
      return it('should error once', function() {
        var errors, pipe, thrower, thru;
        errors = 0;
        thrower = es.through(function(data) {
          return this.emit('error', new Error(data));
        });
        thru = es.through(function(data) {
          return this.emit('data', data);
        });
        pipe = es.pipeline(thru, thrower);
        pipe.on('error', function(err) {
          errors++;
          console.log('error count', errors);
          if (errors.length > 1) {
            return done();
          }
        });
        return pipe.write('meh');
      });
    });
    return describe('reader', function() {
      it('should stream', function(done) {
        var ds, dupe, mainData, mdm1, reader, thru, writer;
        mdm1 = MuxDemux();
        mainData = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20];
        reader = es.readable(function(cnt, callback) {
          if (cnt === mainData.length) {
            return this.emit('end');
          }
          return callback(null, mainData[cnt]);
        });
        writer = es.through(function(data) {
          return console.log("got " + data);
        });
        dupe = es.duplex(writer, reader);
        ds = mdm1.createWriteStream('reply');
        thru = es.through(function(data) {
          return ds.write(data * 2);
        });
        reader.on('end', function() {
          return done();
        });
        return dupe.pipe(thru).pipe(mdm1).pipe(dupe);
      });
      it('should be happy', function(done) {
        var ck, ds, getConn, interpret, main, mdm1;
        mdm1 = MuxDemux();
        ds = mdm1.createWriteStream('reply');
        interpret = es.through(function(data) {
          console.log('data interpret', data);
          return this.emit('data', data.toString());
        });
        getConn = function(cb) {
          var conn;
          conn = net.createConnection(6379, 'localhost');
          return cb(null, conn);
        };
        main = es.through(function(args) {
          var behalf, parsed;
          console.log('received main', args);
          parsed = Redis.parse(args);
          behalf = es.through(function(reply) {
            console.log('onbehalf', reply);
            return main.emit('data', reply);
          });
          return getConn(function(err, conn) {
            conn.pipe(interpret).pipe(behalf);
            return conn.write(parsed);
          });
        });
        ck = es.through(function(data) {
          data.should.equal('+OK\r\n');
          return done();
        });
        main.pipe(ck);
        return main.write(['select', 11]);
      });
      return it('should support tcp', function(done) {
        var parseCommand, reply, stream;
        parseCommand = require('../redis-command-parser');
        reply = es.through(function(reply) {
          return this.emit('data', reply + '');
        });
        stream = es.through(function(cmd) {
          var main;
          console.log('inputcmd', cmd);
          main = net.createConnection(6379, 'localhost');
          main.pipe(reply);
          return main.write(parseCommand(cmd));
        });
        stream.pipe(reply).pipe(es.through(function(reply) {
          console.log('reply2', reply);
          return done();
        }));
        return stream.write(['select', 11]);
      });
    });
  });

}).call(this);
