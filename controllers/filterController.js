const buildEmailPhoneQuery = require('../queries/emailPhoneQuery');
const { logAccess, getAccessMap } = require('../services/accessLogger');
const { peopleDb: db, optionsDb } = require('../config/duckdb');
const { expandDesignations } = require('../utils/expandDesignations');

// Filter API
exports.filterLeads = async (req, res) => {
  const {
    filters = {},
    searchQuery,
    selectAll,
    Designations = [],
    page = 1,
    userId
  } = req.body;

  if (!userId) return res.status(400).json({ error: "userId is required" });

  try {
    const rowIdSet = new Set();

    // Expand Designations if provided
    for (const desig of Designations) {
      const ids = await expandDesignations(desig, undefined, true);
      ids.forEach(id => rowIdSet.add(id));
    }

    // Handle searchQuery if selectAll is true
    if (selectAll && searchQuery) {
      const ids = await expandDesignations(searchQuery, undefined, true);
      ids.forEach(id => rowIdSet.add(id));
    }

    const rowIds = [...rowIdSet];

    // Map camel case to original DuckDB field names
    const fieldMap = {
      organization: "Organization",
      city: "City",
      state: "State",
      country: "Country",
      orgSize: "\"Org Size\"",
      orgIndustry: "\"Org Industry\""
    };

    // Build filter conditions
    const conditions = [];
    for (const [camelCaseField, originalField] of Object.entries(fieldMap)) {
      const values = filters[camelCaseField];
      if (values?.length > 0) {
        const safeVals = values.map(v => `'${v.trim().replace(/'/g, "''")}'`);
        conditions.push(`${originalField} IN (${safeVals.join(', ')})`);
      }
    }

    // Handle row ID filtering
    let filterJoin = '';
    if (rowIds.length > 50000) {
      await new Promise((resolve, reject) => {
        db.serialize(() => {
          db.run(`DELETE FROM filter_ids`, err => {
            if (err) return reject(err);
            const values = rowIds.map(id => `(${Number(id)})`).join(',');
            db.run(`INSERT INTO filter_ids VALUES ${values}`, err => {
              if (err) return reject(err);
              resolve();
            });
          });
        });
      });
      conditions.push(`people.row_id IN (SELECT row_id FROM filter_ids)`);
    } else if (rowIds.length > 0) {
      const inlineRows = rowIds.map(r => `(${Number(r)})`).join(',');
      filterJoin = `, (VALUES ${inlineRows}) AS filter_ids(row_id)`;
      conditions.push(`people.row_id = filter_ids.row_id`);
    }

    // Build the WHERE clause
    const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    // Main data query
    const mainQuery = `
      SELECT row_id, Name, Designation, Email, Phone,
             Organization, City, State, Country,
             "Org Size", "Org Industry"
      FROM people
      ${filterJoin}
      ${whereClause}
      ORDER BY row_id
      LIMIT 25
      OFFSET ${(page - 1) * 25}
    `;

    // Count query
    const countQuery = `
      SELECT COUNT(*) AS total
      FROM people
      ${filterJoin}
      ${whereClause}
    `;

    // Run the main and count queries in parallel
    const [rows, countRows] = await Promise.all([
      new Promise((resolve, reject) => {
        db.all(mainQuery, (err, rows) => (err ? reject(err) : resolve(rows)));
      }),
      new Promise((resolve, reject) => {
        db.all(countQuery, (err, rows) => (err ? reject(err) : resolve(rows)));
      })
    ]);

    // Map access permissions
    const ids = rows.map(r => Number(r.row_id));
    const accessMap = await getAccessMap(userId, ids);

    const cleaned = rows.map(row => {
      const rowId = Number(row.row_id);
      const accessKey = `${userId}_${rowId}`;
      const accessType = accessMap[accessKey];
      return {
        ...row,
        row_id: rowId,
        Email: [1, 3, 4].includes(accessType) ? row.Email : null,
        Phone: [2, 3, 4].includes(accessType) ? row.Phone : null
      };
    });

    res.json({ cleaned, count: Number(countRows[0]?.total || 0) });

  } catch (err) {
    console.error('❌ filterLeads Error:', err);
    res.status(500).json({ error: "Filter processing failed" });
  }
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

// Designation Search
exports.getDesignations = async (req, res) => {
  const { field, query } = req.body;

  // Validate input
  if (!field || !query) {
    return res.status(400).json({ error: 'field and query are required' });
  }

  if (field.toLowerCase() !== 'designation') {
    return res.status(400).json({ error: 'Only "designation" field is supported' });
  }

  if (query.length > 100) {
    return res.status(400).json({ error: 'Query too long' });
  }

  try {
    // Use the same limit as searchOptions for consistency
    const limit = 100;
    const designations = await expandDesignations(query, limit, false);
    res.json(designations);
  } catch (err) {
    console.error('Error expanding designations:', err);
    res.status(500).json({ error: 'Failed to expand designations' });
  }
};