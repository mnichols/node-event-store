util = require 'util'
formatString1 = '*%d\r\n'
formatString2 = '$%d\r\n%s\r\n'
module.exports = (elems) ->
    retval = util.format(formatString1, elems.length)
    while (elems.length) 
        retval += util.format(formatString2, Buffer.byteLength(elems[0]+''), elems.shift()+'')
    retval
