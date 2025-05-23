const duckdb = require('duckdb');
const path = require('path');

const optionsDb = new duckdb.Database(path.resolve(__dirname, '../options.duckdb'));
const peopleDb = new duckdb.Database(path.resolve(__dirname, '../people.duckdb'));
const mapDb = new duckdb.Database(path.resolve(__dirname, '../map.duckdb'));

peopleDb.run("PRAGMA memory_limit='3GB'");
peopleDb.run("PRAGMA threads=4");
peopleDb.run("PRAGMA temp_directory='memory'");
peopleDb.run("PRAGMA enable_object_cache=true");

module.exports = { peopleDb, optionsDb, mapDb };
