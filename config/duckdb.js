const duckdb = require('duckdb');
const path = require('path');
const diskDbPath = path.resolve(__dirname, '../options.duckdb');
const optionsDb = new duckdb.Database(':memory:');

const peopleDb = new duckdb.Database(path.resolve(__dirname, '../people.duckdb'));

optionsDb.run(`ATTACH '${diskDbPath}' AS disk;`, (err) => {
  if (err) return console.error('Failed to attach disk DB:', err);

  optionsDb.run(`CREATE TABLE options AS SELECT * FROM disk.options;`, (err) => {
    if (err) return console.error('Failed to copy options table:', err);
    console.log('options.duckdb loaded into memory');
  });
});

module.exports = { peopleDb, optionsDb };