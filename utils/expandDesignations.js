const path = require('path');
const fs = require('fs');
const levenshtein = require('fast-levenshtein');
const { mapDb: db } = require('../config/duckdb');

const WORDS_PATH = path.resolve(__dirname, '../words.txt');

// Load valid English words
const validWords = fs.readFileSync(WORDS_PATH, 'utf-8')
  .split('\n')
  .map(w => w.trim().toLowerCase())
  .filter(Boolean);

// Acronym → Expansion mapping
const synonymMap = {
  ceo: 'chief executive officer',
  cfo: 'financial officer',
  coo: 'chief operating officer',
  cto: 'chief technical officer',
  cmo: 'chief marketing officer',
  cio: 'chief information officer',
  chro: 'chief human resources officer',
  cso: 'chief strategy officer',
  cpo: 'chief product officer chief people officer',
  cao: 'chief administrative officer',
  cdo: 'chief data officer chief digital officer',
  ciso: 'chief information security officer',
  hr: 'human resources',
  vp: 'vice president',
  svp: 'senior vice president',
  avp: 'assistant vice president associate vice president',
  md: 'managing director',
  gm: 'general manager',
  bd: 'business development',
  bdm: 'business development manager',
  sdr: 'sales development representative',
  bdr: 'business development representative',
  ae: 'account executive',
  am: 'account manager',
  sam: 'senior account manager',
  pm: 'product manager project manager',
  tpm: 'technical program manager',
  sde: 'software development engineer',
  swe: 'software engineer',
  qa: 'quality assurance',
  ux: 'user experience designer',
  ui: 'user interface designer',
  uxui: 'user experience user interface designer',
  cs: 'customer success',
  csm: 'customer success manager',
  crm: 'customer relationship manager',
  pr: 'public relations',
  pa: 'personal assistant',
  ea: 'executive assistant',
  mktg: 'marketing',
  'r&d': 'research and development',
  devops: 'development operations engineer'
};

// Fuzzy match: only check validWords with same first char
function fuzzyMatch(word) {
  const first = word[0];
  const len = word.length;

  const candidates = validWords.filter(w =>
    w.length >= len - 1 &&
    w.length <= len + 1 &&
    w[0] === first
  );

  return candidates
    .map(w => ({ word: w, dist: levenshtein.get(word, w) }))
    .filter(d => d.dist <= 2)
    .sort((a, b) => a.dist - b.dist)
    .slice(0, 3)
    .map(d => d.word);
}

exports.expandDesignations = async (searchQuery, count, returnRowIds = false) => {
  if (!searchQuery) return [];

  const cleanedWords = searchQuery
    .replace(/[^\w\s]/gi, '')
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);

  if (cleanedWords.length === 0) return [];

  const expandedWords = cleanedWords.flatMap(w =>
    synonymMap[w] ? synonymMap[w].split(' ') : [w]
  );

  const wordGroupList = [];
  expandedWords.forEach((word, groupIndex) => {
    const matches = fuzzyMatch(word);
    matches.forEach(m => {
      wordGroupList.push({ word: m, group: groupIndex });
    });
  });

  if (wordGroupList.length === 0) return [];

  let rows = [];

  try {
    rows = await new Promise((resolve, reject) => {
      db.serialize(() => {
        db.run(`DROP TABLE IF EXISTS temp_words;`, err => {
          if (err) return reject(err);

          db.run(`CREATE TEMP TABLE temp_words(word TEXT, group_index INTEGER);`, err => {
            if (err) return reject(err);

            const stmt = db.prepare(`INSERT INTO temp_words VALUES (?, ?)`);
            for (const { word, group } of wordGroupList) {
              stmt.run(word, group);
            }
            stmt.finalize();

            const numGroups = expandedWords.length;

            const query = returnRowIds
              ? `
                SELECT row_id
                FROM map
                JOIN temp_words ON map.word = temp_words.word
                GROUP BY row_id
                HAVING COUNT(DISTINCT temp_words.group_index) = ${numGroups}
              `
              : `
                SELECT row_id, designation
                FROM map
                JOIN temp_words ON map.word = temp_words.word
                GROUP BY row_id, designation
                HAVING COUNT(DISTINCT temp_words.group_index) = ${numGroups}
              `;

            db.all(query, (err, result) => {
              if (err) return reject(err);
              resolve(result);
            });
          });
        });
      });
    });
  } catch (err) {
    console.error('🔴 Error expanding designations:', err);
    return [];
  }

  // Only return row_ids directly
  if (returnRowIds) {
    return rows.map(r => r.row_id);
  }

  // Deduplicate by designation
  const seen = new Set();
  const result = [];

  for (const row of rows) {
    const key = row.designation.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      result.push(row.designation);
      if (typeof count === 'number' && result.length >= count) {
        break;
      }
    }
  }

  return result;
};
