const duckdb = require('duckdb');
const path = require('path');
const fs = require('fs');
const { peopleDb } = require('../config/duckdb');
const { getAccessMap } = require('../services/accessLogger');
const { v4: uuidv4 } = require('uuid');

const dbMap = {};
const queueMap = {};

const getOrCreateDb = (userId) => {
  const filePath = path.resolve(__dirname, '../user_lists', `${userId}.duckdb`);
  if (!dbMap[userId]) {
    dbMap[userId] = new duckdb.Database(filePath);
  }
  return dbMap[userId];
};

const withUserDb = async (userId, callback) => {
  if (!queueMap[userId]) queueMap[userId] = Promise.resolve();
  queueMap[userId] = queueMap[userId].then(() => callback(getOrCreateDb(userId)));
  return queueMap[userId];
};

// GET LIST SUMMARY
exports.getListSummary = async (req, res) => {
  const { userId } = req.body;
  if (!userId) return res.status(400).json({ error: 'userId is required' });

  const filePath = path.resolve(__dirname, '../user_lists', `${userId}.duckdb`);
  if (!fs.existsSync(filePath)) return res.json([]);

  try {
    const rows = await withUserDb(userId, (db) => {
      return new Promise((resolve, reject) => {
        db.run(`
          CREATE TABLE IF NOT EXISTS user_lists (
            list_name TEXT,
            row_id INTEGER
          );
        `, (err) => {
          if (err) return reject(err);
          db.all(`
            SELECT list_name AS name, COUNT(CASE WHEN row_id != -1 THEN 1 END) AS total
            FROM user_lists
            GROUP BY list_name
            ORDER BY list_name;
          `, (err, rows) => err ? reject(err) : resolve(rows));
        });
      });
    });

    res.json(
      rows.map(row => ({
        name: row.name,
        total: Number(row.total)
      }))
    );
  } catch (err) {
    console.error("Summary API failed:", err);
    res.status(500).json({ error: 'Failed to fetch list summary' });
  }
};

// STORE LIST
exports.storeList = async (req, res) => {
  const { userId, listName, rowIds } = req.body;
  if (!userId || !listName || !Array.isArray(rowIds)) {
    return res.status(400).json({ error: "userId, listName and rowIds[] are required" });
  }

  const sanitizedListName = listName.replace(/'/g, "''");

  await withUserDb(userId, async (userDb) => {
    const existingRows = await new Promise((resolve, reject) => {
      userDb.all(`
        CREATE TABLE IF NOT EXISTS user_lists (
          list_name TEXT,
          row_id INTEGER
        );
        SELECT row_id FROM user_lists WHERE list_name = '${sanitizedListName}';
      `, (err, rows) => err ? reject(err) : resolve(rows));
    });

    const existingIds = new Set(existingRows.map(r => r.row_id));
    const newRowIds = rowIds.filter(id => !existingIds.has(id));

    if (newRowIds.length === 0) {
      return res.json({ message: "All row_ids already exist in this list", inserted: 0 });
    }

    const insertQuery = `
      INSERT INTO user_lists (list_name, row_id)
      VALUES ${newRowIds.map(id => `('${sanitizedListName}', ${id})`).join(', ')};
    `;

    await new Promise((resolve, reject) => {
      userDb.run(insertQuery, (err) => err ? reject(err) : resolve());
    });

    res.json({ message: "List updated", inserted: newRowIds.length });
  });
};

// SHOW LIST
exports.showList = async (req, res) => {
  const { userId, listName, page = 1 } = req.body;
  if (!userId || !listName) {
    return res.status(400).json({ error: "userId and listName are required" });
  }

  const sanitizedListName = listName.replace(/'/g, "''");
  const offset = (page - 1) * 50;

  await withUserDb(userId, async (userDb) => {
    const rowResults = await new Promise((resolve, reject) => {
      userDb.all(`
        CREATE TABLE IF NOT EXISTS user_lists (
          list_name TEXT,
          row_id INTEGER
        );
        SELECT row_id FROM user_lists
        WHERE list_name = '${sanitizedListName}' AND row_id != -1
        ORDER BY row_id
        LIMIT 50 OFFSET ${offset};
      `, (err, rows) => err ? reject(err) : resolve(rows));
    });

    if (!rowResults || rowResults.length === 0) return res.json([]);

    const rowIds = rowResults.map(r => r.row_id);
    const rowIdStr = rowIds.join(',');

    const peopleRows = await new Promise((resolve, reject) => {
      peopleDb.all(`
        SELECT row_id, Name, Designation_Group AS Designation, Email, Phone, Organization, City, State, Country
        FROM people
        WHERE row_id IN (${rowIdStr});
      `, (err, rows) => err ? reject(err) : resolve(rows));
    });

    const accessMap = await getAccessMap(userId, rowIds);

    const cleaned = peopleRows.map(row => {
      const rowId = Number(row.row_id);
      const accessKey = `${userId}_${rowId}`;
      const accessType = accessMap[accessKey];
      return {
        ...row,
        row_id: rowId,
        Email: [1, 3, 4].includes(accessType) ? row.Email : null,
        Phone: [2, 3, 4].includes(accessType) ? row.Phone : null,
      };
    });

    res.json(cleaned);
  });
};

// CREATE EMPTY LIST
exports.createEmptyList = async (req, res) => {
  const { userId, listName } = req.body;
  if (!userId || !listName) {
    return res.status(400).json({ error: "userId and listName are required" });
  }

  const sanitizedListName = listName.replace(/'/g, "''");

  await withUserDb(userId, async (userDb) => {
    const exists = await new Promise((resolve, reject) => {
      userDb.all(
        `CREATE TABLE IF NOT EXISTS user_lists (
           list_name TEXT,
           row_id INTEGER
         );
         SELECT 1 FROM user_lists WHERE list_name = '${sanitizedListName}' LIMIT 1;
        `,
        (err, rows) => err ? reject(err) : resolve(rows.length > 0)
      );
    });

    if (exists) {
      return res.status(400).json({ error: "List already exists" });
    }

    await new Promise((resolve, reject) => {
      userDb.run(
        `INSERT INTO user_lists (list_name, row_id) VALUES ('${sanitizedListName}', -1);`,
        (err) => err ? reject(err) : resolve()
      );
    });

    res.json({ message: `Empty list '${listName}' created successfully` });
  });
};

// EXPORT LIST
exports.queueExportJob = async (req, res) => {
  const { userId, listName, email } = req.body;

  if (!userId || !listName || !email) {
    return res.status(400).json({ error: 'userId, listName, and email are required' });
  }

  const exportDir = path.resolve(__dirname, '../export_jobs');
  if (!fs.existsSync(exportDir)) fs.mkdirSync(exportDir);

  try {
    await withUserDb(userId, async (userDb) => {
      const sanitizedListName = listName.replace(/'/g, "''");

      const rowResults = await new Promise((resolve, reject) => {
        userDb.all(
          `SELECT row_id FROM user_lists WHERE list_name = '${sanitizedListName}' AND row_id != -1`,
          (err, rows) => err ? reject(err) : resolve(rows)
        );
      });

      const rowIds = rowResults.map(r => r.row_id);
      if (!rowIds.length) return res.status(404).json({ error: 'List is empty' });

      const jobId = uuidv4();
      const jobData = { jobId, userId, listName, email, rowIds };
      const filePath = path.join(exportDir, `${jobId}.json`);
      fs.writeFileSync(filePath, JSON.stringify(jobData));

      res.json({ message: 'Your export has been queued and will be emailed shortly.' });
    });
  } catch (err) {
    console.error('Export job queueing failed:', err);
    res.status(500).json({ error: 'Failed to queue export' });
  }
};
