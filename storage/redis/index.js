require('coffee-script')
redisStreamStorage = require('./redis-stream-storage')
module.exports = {
    createClient: redisStreamStorage.createClient
    ,createStorage: redisStreamStorage.createStorage
    ,createAuditor: require('./redis-auditor').createAuditor
}
