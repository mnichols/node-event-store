es = require 'event-stream'
util = require 'util'
{Writable} = require 'readable-stream'

### receives `filter` objects and should 
# support backpressure causes by underlying redis-streamers
# 1. obtain commitCount (zcount) based on filter
# 2. emit header with commitCount (expected number of packets)
# 3. pump range (zrangebyscore) of events based on filter
# 4. for each commit, either emit each event in the commit separately (flatten) or emit each whole commit
# Backpressure will be caused by redis-stream, so clients pipeing
# into this reader will need to handle this
###
Reader = (opts) ->
    Writable.apply @, arguments
    @buffer = []









