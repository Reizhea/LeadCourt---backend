const express = require('express');
const router = express.Router();
const {
  filterLeads,
  getEmailOrPhone,
  searchOptions
} = require('../controllers/filterController');

/**
 * @swagger
 * tags:
 *   name: Filter
 *   description: APIs for filtering, unlocking, exporting, and counting leads
 */
/**
 * @swagger
 * /api/filter:
 *   post:
 *     summary: Filter leads based on fields like designation, location, and organization
 *     tags: [Filter]
 *     description: |
 *       Returns a paginated list of leads based on provided filters.
 *       - Use `page` to paginate (default 50 per page).
 *       - Requires `userId` to check access and control email/phone visibility.
 *       - Returns total matching count alongside the paginated list.
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [filters, page, userId]
 *             properties:
 *               filters:
 *                 type: object
 *                 example: { "City": ["New York"], "Country": ["United States"], "Designation_Group": ["Software Engineer"] }
 *               page:
 *                 type: integer
 *                 example: 1
 *               userId:
 *                 type: string
 *                 example: user123
 *     responses:
 *       200:
 *         description: Filtered list of leads with total count
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 cleaned:
 *                   type: array
 *                   items:
 *                     type: object
 *                     properties:
 *                       row_id:
 *                         type: integer
 *                       Name:
 *                         type: string
 *                       Designation:
 *                         type: string
 *                       Email:
 *                         type: string
 *                       Phone:
 *                         type: string
 *                       Organization:
 *                         type: string
 *                       City:
 *                         type: string
 *                       State:
 *                         type: string
 *                       Country:
 *                         type: string
 *                 count:
 *                   type: integer
 *                   description: Total number of leads matching filters
 *                   example: 125443
 */
router.post('/', filterLeads);

/**
 * @swagger
 * /api/filter/row-access:
 *   post:
 *     summary: Unlock email or phone for specific rows
 *     tags: [Filter]
 *     description: |
 *       Unlocks access to email, phone, or both for the given row_ids and logs it in MongoDB.
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [row_ids, type, userId]
 *             properties:
 *               row_ids:
 *                 type: array
 *                 items:
 *                   type: integer
 *                 example: [3344, 4786]
 *               type:
 *                 type: string
 *                 enum: [email, phone, both]
 *                 example: both
 *               userId:
 *                 type: string
 *                 example: user123
 *     responses:
 *       200:
 *         description: Returns unlocked email and/or phone for each row
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   row_id:
 *                     type: integer
 *                   Email:
 *                     type: string
 *                   Phone:
 *                     type: string
 *             example:
 *               - row_id: 3344
 *                 Email: "john.doe@example.com"
 *                 Phone: "+1234567890"
 */
router.post('/row-access', getEmailOrPhone);

/**
 * @swagger
 * /api/filter/search-options:
 *   post:
 *     summary: Search for partial matches from filter values
 *     tags: [Filter]
 *     description: |
 *       Used for autocomplete dropdowns. Searches values from the Options.parquet file.
 *       - For Designation, it returns both designation and its group.
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [field, query]
 *             properties:
 *               field:
 *                 type: string
 *                 example: Designation
 *               query:
 *                 type: string
 *                 example: Chief
 *     responses:
 *       200:
 *         description: Matching values
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 oneOf:
 *                   - type: object
 *                     properties:
 *                       designation:
 *                         type: string
 *                       designation_group:
 *                         type: string
 *                   - type: object
 *                     properties:
 *                       Value:
 *                         type: string
 */
router.post('/search-options', searchOptions);

module.exports = router;