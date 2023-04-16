var mongoose = require('mongoose');

var QueueSchema = new mongoose.Schema({
    sql: String,
    node: Number
});

module.exports = mongoose.model('Account', QueueSchema);
