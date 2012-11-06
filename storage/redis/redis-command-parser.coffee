util = require 'util'
formatString1 = '*%d\r\n'
formatString2 = '$%d\r\n%s\r\n'
module.exports = (elems) ->
    cmd = elems
    retval = util.format(formatString1, cmd.length)
    while (elems.length) 
        retval += util.format(formatString2, Buffer.byteLength(cmd[0]+''), cmd.shift()+'')
    retval
