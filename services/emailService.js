const nodemailer = require('nodemailer');
const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: Number(process.env.SMTP_PORT),
  secure: process.env.SMTP_SECURE === 'true',
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS,
  },
});
exports.sendCSVEmail = async (to, csvBuffer, filename = 'export.csv') => {
  return transporter.sendMail({
    from: `"Lead Export" <${process.env.SMTP_USER}>`,
    to,
    subject: 'Your CSV Export',
    text: 'Here is the export you requested.',
    attachments: [
      {
        filename,
        content: csvBuffer,
      },
    ],
  });
};