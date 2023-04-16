var mongoose = require('mongoose');

var QueueSchema = new mongoose.Schema({
    sql: String,
    node: Number,
    insert: Boolean,
    oldid: Number,
    name: String,
    year: Number,
    rank: Number,
    genres: String,
    director: String
});

module.exports = mongoose.model('Account', QueueSchema);
