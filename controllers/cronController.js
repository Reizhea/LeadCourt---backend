const fs = require('fs');
const path = require('path');
const { Parser } = require('json2csv');
const { logAccessBatch } = require('../services/accessLogger');
const { sendCSVEmail } = require('../services/emailService');
const { peopleDb } = require('../config/duckdb');

const jobDir = path.resolve(__dirname, '../export_jobs');
const failedDir = path.resolve(__dirname, '../failed_jobs');
const userListDir = path.resolve(__dirname, '../user_lists');

if (!fs.existsSync(failedDir)) fs.mkdirSync(failedDir);

// export cron job
exports.runExportJobs = async (req, res) => {
  let processedCount = 0;

  const processDir = async (dirPath, isRetry = false) => {
    const files = fs.readdirSync(dirPath).filter(f => f.endsWith('.json'));

    for (const file of files) {
      const jobPath = path.join(dirPath, file);
      const job = JSON.parse(fs.readFileSync(jobPath, 'utf-8'));

      try {
        const rowIdStr = job.rowIds.join(',');
        const rows = await new Promise((resolve, reject) => {
          peopleDb.all(
            `SELECT * FROM people WHERE row_id IN (${rowIdStr})`,
            (err, result) => err ? reject(err) : resolve(result)
          );
        });

        const chunkSize = 1000;
        for (let i = 0; i < job.rowIds.length; i += chunkSize) {
          const chunk = job.rowIds.slice(i, i + chunkSize);
          await logAccessBatch(job.userId, chunk, 4);
        }

        const finalRows = rows.map(r => ({
          Name: r.Name,
          Designation: r.Designation,
          Email: r.Email,
          Phone: r.Phone,
          'LinkedIn URL': r['LinkedIn URL'],
          Organization: r.Organization,
          City: r.City,
          State: r.State,
          Country: r.Country,
          'Org Size': r['Org Size'],
          'Org Industry': r['Org Industry'],
        }));

        const parser = new Parser();
        const csv = parser.parse(finalRows);
        const buffer = Buffer.from(csv, 'utf-8');

        await sendCSVEmail(job.email, buffer, `${job.listName}.csv`);
        fs.unlinkSync(jobPath);
        console.log(`${isRetry ? 'Retried' : 'Processed'} export: ${job.jobId}`);
        processedCount++;
      } catch (err) {
        console.error(`${isRetry ? 'Retry' : 'Export'} failed for job ${file}:`, err);
        if (!isRetry) {
          fs.renameSync(jobPath, path.join(failedDir, file));
        }
      }
    }
  };

  await processDir(jobDir, false); 
  await processDir(failedDir, true);

  if (processedCount > 0) {
    res.json({ done: true, processed: processedCount });
  } else {
    res.status(204).end();
  }
};

// checkpoint cron job
exports.runCheckpoint = async (req, res) => {
  peopleDb.run('CHECKPOINT;', (err) => {
    if (err) console.error('Access log checkpoint failed:', err);
    else console.log('Access log checkpointed');
  });

  const userDbs = fs.readdirSync(userListDir).filter(f => f.endsWith('.duckdb'));
  for (const file of userDbs) {
    const fullPath = path.join(userListDir, file);
    try {
      await new Promise((resolve, reject) => {
        const { spawn } = require('child_process');
        const proc = spawn('duckdb', [fullPath], { stdio: ['pipe', 'ignore', 'ignore'] });
        proc.stdin.write('CHECKPOINT;\n');
        proc.stdin.end();
        proc.on('exit', () => resolve());
        proc.on('error', reject);
      });
      console.log(`Checkpointed ${file}`);
    } catch (e) {
      console.error(`Failed to checkpoint ${file}:`, e);
    }
  }
  res.json({ success: true });
};