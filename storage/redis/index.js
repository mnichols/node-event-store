require('coffee-script')
module.exports = {
    createStorage: require('./redis-stream-storage').createStorage
    ,createAuditor: require('./redis-auditor').createAuditor
}
