const express = require('express');
const router = express.Router();
const {
  filterLeads,
  getEmailOrPhone,
  searchOptions,
  getDesignations
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
 *     summary: Filter leads using fuzzy designation search and other lead fields
 *     tags: [Filter]
 *     description: |
 *       Returns a paginated list of leads filtered by fuzzy designation matches and other lead fields.
 *       - `searchQuery` and `Designations` are expanded into internal row_ids for efficient filtering.
 *       - Filters are applied on location, organization, and other lead fields.
 *       - Email/Phone are masked unless user has access.
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
 *                 description: Filters for various lead fields
 *                 example:
 *                   organization: ["Google"]
 *                   city: ["New York"]
 *                   state: ["New York"]
 *                   country: ["United States"]
 *                   orgSize: ["10000-25000"]
 *                   orgIndustry: ["Tech"]
 *               searchQuery:
 *                 type: string
 *                 description: Fuzzy search term for designation
 *                 example: "ceo"
 *               Designations:
 *                 type: array
 *                 description: List of specific designations to filter by
 *                 items:
 *                   type: string
 *                 example: ["Chief Executive Officer"]
 *               selectAll:
 *                 type: boolean
 *                 description: Whether to select all rows matching the searchQuery
 *                 example: true
 *               page:
 *                 type: integer
 *                 description: Page number for pagination
 *                 example: 1
 *               userId:
 *                 type: string
 *                 description: Unique user ID for access control
 *                 example: "user_abc"
 *     responses:
 *       200:
 *         description: Filtered and masked leads with total count
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
 *                       orgSize:
 *                         type: string
 *                       orgIndustry:
 *                         type: string
 *                       City:
 *                         type: string
 *                       State:
 *                         type: string
 *                       Country:
 *                         type: string
 *                 count:
 *                   type: integer
 *                   description: Total number of filtered leads
 *                   example: 108937
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

/**
 * @swagger
 * /api/filter/designations:
 *   post:
 *     summary: Fuzzy match user input to standard designations
 *     tags: [Filter]
 *     description: |
 *       Expands a user input like "ceo" or "chief" into possible standard designations
 *       using synonym mapping. This endpoint accepts the same payload as /api/filter/search-options.
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
 *                 description: The field to search (must be "designation")
 *                 example: "designation"
 *               query:
 *                 type: string
 *                 description: The partial or full designation to expand
 *                 example: "ceo"
 *     responses:
 *       200:
 *         description: Matching designations
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: string
 *               example: ["chief executive officer", "chief operations officer", "ceo", "chief executive"]
 *       400:
 *         description: Bad Request
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 error:
 *                   type: string
 *                   example: "field and query are required"
 *       500:
 *         description: Internal Server Error
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 error:
 *                   type: string
 *                   example: "Failed to expand designations"
 */
router.post('/designations', getDesignations);

module.exports = router;