const duckdb = require('duckdb');
const path = require('path');
const { withAccessQueue } = require('../utils/accessQueueMap');
const accessDb = new duckdb.Database(path.resolve(__dirname, '../access_logs.duckdb'));

accessDb.run(`
  CREATE TABLE IF NOT EXISTS access_logs (
    userId TEXT,
    row_id INTEGER,
    accessType INTEGER,
    PRIMARY KEY(userId, row_id)
);
`);

exports.logAccess = async (userId, row_id, newVal) => {
  await withAccessQueue(userId, row_id, async () => {
    const stmt = `
      INSERT INTO access_logs (userId, row_id, accessType)
      VALUES ('${userId}', ${row_id}, ${newVal})
      ON CONFLICT(userId, row_id)
      DO UPDATE SET accessType = 
        CASE 
          WHEN access_logs.accessType = 1 AND ${newVal} = 2 THEN 3
          WHEN access_logs.accessType = 2 AND ${newVal} = 1 THEN 3
          WHEN access_logs.accessType = 3 THEN 3
          WHEN access_logs.accessType = 4 THEN 4
          ELSE ${newVal}
        END;
    `;

    return new Promise((resolve, reject) => {
      accessDb.run(stmt, (err) => (err ? reject(err) : resolve()));
    });
  });
};

exports.getAccessMap = async (userId, rowIds = []) => {
    if (!rowIds.length) return {};
  
    const placeholders = rowIds.join(',');
    const stmt = `
      SELECT row_id, accessType
      FROM access_logs
      WHERE userId = '${userId}' AND row_id IN (${placeholders});
    `;
  
    return new Promise((resolve, reject) => {
      accessDb.all(stmt, (err, rows) => {
        if (err) return reject(err);
        const map = {};
        for (const row of rows) {
          map[`${userId}_${row.row_id}`] = row.accessType;
        }
        resolve(map);
      });
    });
  };
  
  exports.logAccessBatch = async (userId, rowIds, newVal = 4) => {
    if (!Array.isArray(rowIds) || !rowIds.length) return;
  
    const values = rowIds
      .map(id => `('${userId}', ${id}, ${newVal})`)
      .join(',');
  
    const stmt = `
      INSERT INTO access_logs (userId, row_id, accessType)
      VALUES ${values}
      ON CONFLICT(userId, row_id)
      DO UPDATE SET accessType = 
        CASE 
          WHEN access_logs.accessType = 4 THEN 4
          ELSE ${newVal}
        END;
    `;
  
    return new Promise((resolve, reject) => {
      accessDb.run(stmt, (err) => (err ? reject(err) : resolve()));
    });
  };