module.exports = (storage, admin) ->
    storage.on "#{storage.id}.commit", admin.audit commit


