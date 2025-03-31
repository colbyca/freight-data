import express, { Request, Response } from 'express';
import { QueueService } from '../services/queue';
import { z } from 'zod';

const router = express.Router();
const queueService = new QueueService();

// Schema for validating the request body
const locationQuerySchema = z.object({
    month: z.string().regex(/^month_\d{2}_routes$/),
    bounds: z.object({
        west: z.number(),
        south: z.number(),
        east: z.number(),
        north: z.number()
    })
});

// Schema for validating the development SQL query
const devQuerySchema = z.object({
    query: z.string().min(1)
});

// Initialize queue connection
queueService.connect().catch(console.error);

router.post('/location', async (req: Request, res: Response) => {
    try {
        // Validate request body
        const { month, bounds } = locationQuerySchema.parse(req.body);

        // TODO: parameterize the query. because we are using zod, sql injection should not be an issue, but it's good practice to do so
        const query = `
            SELECT *
            FROM ${month}
            WHERE ST_Within(
                location::geometry,
                ST_MakeEnvelope(${bounds.west}, ${bounds.south}, ${bounds.east}, ${bounds.north}, 4326)
            ) LIMIT 10;
        `;

        // Submit the query to the queue
        const job = await queueService.submitQuery(query);

        res.json({
            jobId: job.id,
            status: job.status,
            message: 'Query submitted successfully'
        });
    } catch (error) {
        if (error instanceof z.ZodError) {
            res.status(400).json({
                error: 'Invalid request format',
                details: error.errors
            });
        } else {
            console.error('Error submitting query:', error);
            res.status(500).json({
                error: 'Failed to submit query'
            });
        }
    }
});

// Endpoint to check query status
router.get('/status/:jobId', async (req: Request, res: Response) => {
    try {
        const job = await queueService.getQueryStatus(req.params.jobId);
        if (!job) {
            res.status(404).json({ error: 'Query job not found' });
        }
        res.json(job);
    } catch (error) {
        console.error('Error checking query status:', error);
        res.status(500).json({ error: 'Failed to check query status' });
    }
});

// TODO: remove this endpoint after development
// DANGEROUS ENDPOINT: allows you to execute arbitrary SQL queries
// I am adding this endpoint to make it easier to develop the frontend, but once we start
// getting a better idea of what endpoints are needed, this should be removed.
router.post('/dev/execute', async (req: Request, res: Response) => {
    try {
        // Validate request body
        const { query } = devQuerySchema.parse(req.body);

        // Basic protection against DROP TABLE statements
        const normalizedQuery = query.toLowerCase().trim();
        if (normalizedQuery.includes('drop table')) {
            res.status(403).json({
                error: 'DROP TABLE statements are not allowed for safety reasons'
            });
        }

        // Submit the query to the queue
        const job = await queueService.submitQuery(query);

        res.json({
            jobId: job.id,
            status: job.status,
            message: 'Query submitted successfully'
        });
    } catch (error) {
        if (error instanceof z.ZodError) {
            res.status(400).json({
                error: 'Invalid request format',
                details: error.errors
            });
        } else {
            console.error('Error submitting development query:', error);
            res.status(500).json({
                error: 'Failed to submit query'
            });
        }
    }
});

export default router; 