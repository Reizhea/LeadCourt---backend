require('dotenv').config();
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const filterRoute = require('./routes/filterRoute');
const listRoute = require('./routes/listRoute');
const swaggerUi = require('swagger-ui-express');
const swaggerSpec = require('./config/swagger');
const cronRoutes = require('./routes/cronRoute');
const app = express();

app.use(cors({
  origin: '*',
}));
app.use(bodyParser.json());

app.use('/api/filter', filterRoute);
app.use('/api/cron', cronRoutes);
app.use('/api/list', listRoute);
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpec));
app.get('/swagger.json', (req, res) => {
  res.setHeader('Content-Type', 'application/json');
  res.send(swaggerSpec);
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, '0.0.0.0', () => console.log(`Server running on port ${PORT}`));