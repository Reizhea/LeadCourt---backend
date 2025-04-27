const duckdb = require('duckdb');
const path = require('path');

const optionsDb = new duckdb.Database(path.resolve(__dirname, '../options.duckdb'));
const peopleDb = new duckdb.Database(path.resolve(__dirname, '../people.duckdb'));

module.exports = { peopleDb, optionsDb };
