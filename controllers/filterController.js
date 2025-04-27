const buildFilterQuery = require('../queries/filterQuery');
const getFilteredCount = require('../utils/getFilteredCount');
const buildEmailPhoneQuery = require('../queries/emailPhoneQuery');
const { logAccess, getAccessMap } = require('../services/accessLogger');
const { peopleDb: db, optionsDb } = require('../config/duckdb');

// Filter API
exports.filterLeads = async (req, res) => {
  const { filters = {}, page = 1, userId } = req.body;

  if (!userId) return res.status(400).json({ error: "userId is required" });

  const query = buildFilterQuery(page, filters);

  db.all(query, async (err, rows) => {
    if (err) return res.status(500).json({ error: 'Query failed' });

    try {
      const rowIds = rows.map(r => Number(r.row_id));
      const accessMap = await getAccessMap(userId, rowIds);

      const cleaned = rows.map(row => {
        const rowId = Number(row.row_id);
        const accessKey = `${userId}_${rowId}`;
        const accessType = accessMap[accessKey];

        const email = [1, 3, 4].includes(accessType) ? row.Email : null;
        const phone = [2, 3, 4].includes(accessType) ? row.Phone : null;

        return {
          ...row,
          row_id: rowId,
          Email: email,
          Phone: phone
        };
      });
      const total = await getFilteredCount(filters);
      res.json({cleaned, count: total });
    } catch (err) {
      console.error("AccessLog lookup failed:", err);
      res.status(500).json({ error: "Failed to enrich lead data" });
    }
  });
};

// Get Email or Phone
exports.getEmailOrPhone = async (req, res) => {
  const { row_ids, type, userId } = req.body;
  if (!Array.isArray(row_ids) || !type || !userId) {
    return res.status(400).json({ error: 'row_ids (array), type, and userId are required' });
  }

  const results = [];

  for (const row_id of row_ids) {
    const query = buildEmailPhoneQuery(row_id, type);

    try {
      const rows = await new Promise((resolve, reject) => {
        db.all(query, (err, rows) => (err ? reject(err) : resolve(rows)));
      });

      const result = rows[0] || {};
      results.push({ row_id, ...result });

      let newVal = type === 'email' ? 1 : type === 'phone' ? 2 : 3;
      await logAccess(userId, row_id, newVal);

    } catch (err) {
      console.error(`Error for row_id ${row_id}:`, err);
    }
  }

  res.json(results);
};

//Search Params
exports.searchOptions = (req, res) => {
  const { field, query, page = 1 } = req.body;

  if (!field || !query) {
    return res.status(400).json({ error: 'field and query are required' });
  }

  if (query.length > 100) {
    return res.status(400).json({ error: 'Query too long' });
  }

  const safeField = field.replace(/[^a-zA-Z_]/g, '');
  const safeQuery = query.replace(/'/g, "''").toLowerCase();
  const limit = 100;

  const sql = `
    SELECT DISTINCT Value
    FROM options
    WHERE LOWER(Field) = '${safeField.toLowerCase()}'
      AND LOWER(Value) LIKE '%${safeQuery}%'
    ORDER BY LOWER(Value) = '${safeQuery}' DESC
    LIMIT ${limit};
  `;

  optionsDb.all(sql, (err, rows) => {
    if (err) {
      console.error('Search failed:', err);
      return res.status(500).json({ error: 'Search failed' });
    }

    return res.json(rows.map(r => r.Value));
  });
};
