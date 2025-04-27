const { peopleDb: db } = require('../config/duckdb');

module.exports = function getFilteredCount(filters = {}) {
  const allowedFields = [
    'Designation',
    'Email Status',
    'Organization',
    'City',
    'State',
    'Country'
  ];

  const conditions = [];

  for (const field of allowedFields) {
    const values = filters[field];
    if (values && Array.isArray(values) && values.length > 0) {
      const safeVals = values
        .map(v => `'${v.toLowerCase().replace(/'/g, "''")}'`)
        .join(', ');

      if (field === 'Designation') {
        conditions.push(`(LOWER(Designation) IN (${safeVals}) OR LOWER(Designation_Group) IN (${safeVals}))`);
      } else {
        conditions.push(`LOWER("${field}") IN (${safeVals})`);
      }
    }
  }

  const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const query = `SELECT COUNT(*) AS total FROM people ${whereClause}`;

  return new Promise((resolve, reject) => {
    db.all(query, (err, rows) => {
      if (err) return reject(err);
      resolve(Number(rows[0]?.total || 0));
    });
  });
};
