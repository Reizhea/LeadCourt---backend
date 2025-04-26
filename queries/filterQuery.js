module.exports = function buildFilterQuery(page = 1, filters = {}) {
  const allowedFields = [
    'Designation_Group',
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
      conditions.push(`LOWER("${field}") IN (${safeVals})`);
    }
  }

  const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const limit = 50;
  const offset = (page - 1) * limit;

  return `
    SELECT row_id, Name, Designation_Group AS Designation, Email, Phone, Organization, City, State, Country
    FROM people
    ${whereClause}
    ORDER BY row_id
    LIMIT ${limit}
    OFFSET ${offset}
  `;
};