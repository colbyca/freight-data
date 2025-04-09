import express, { Request, Response } from 'express';
import { QueueService } from '../services/queue';
import { date, z } from 'zod';

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

// Schema for validating the heatmap request
const heatmapQuerySchema = z.object({
    month: z.number().min(1).max(12),
    startDate: z.string().datetime(),
    endDate: z.string().datetime(),
    eps: z.number().positive(),
    minSamples: z.number().int().positive()
});

// Schema for validating the Utah boundary request
const utahBoundarySchema = z.object({
    startDate: z.string().datetime(),
    endDate: z.string().datetime(),
    month: z.number().min(1).max(12),
}).refine((data) => {
    // Check if the date range is within the specified month
    const start = new Date(data.startDate);
    const end = new Date(data.endDate);
    const monthsMatch = start.getMonth() === (data.month - 1);
    return start.getMonth() === end.getMonth() && start.getFullYear() === end.getFullYear() && monthsMatch;
}, {
    message: `Date range must be within the specified month`,
    path: ["startDate", "endDate"],
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

        const job = await queueService.submitQuery(query, {
            type: 'regular'
        });

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
            return;
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

        // Submit the query to the queue with explicit type
        const job = await queueService.submitQuery(query, {
            type: 'regular'
        });

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

router.post('/heatmap', async (req: Request, res: Response) => {
    try {
        // Validate request body
        const { month, startDate, endDate, eps, minSamples } = heatmapQuerySchema.parse(req.body);

        // Create the query to fetch data from the database
        const query = `
            SELECT 
                stop_id,
                ST_Y(location::geometry) as latitude,
                ST_X(location::geometry) as longitude,
                start_time,
                end_time,
                duration_minutes
            FROM month_${month.toString().padStart(2, '0')}_stops
            WHERE start_time >= '${startDate}'
            AND end_time <= '${endDate}';
        `;
        // Submit the query to the queue with additional parameters
        const job = await queueService.submitQuery(query, {
            type: 'heatmap',
            params: {
                eps,
                minSamples
            }
        });

        res.json({
            jobId: job.id,
            status: job.status,
            message: 'Heatmap generation job submitted successfully'
        });
    } catch (error) {
        if (error instanceof z.ZodError) {
            res.status(400).json({
                error: 'Invalid request format',
                details: error.errors
            });
        } else {
            console.error('Error submitting heatmap query:', error);
            res.status(500).json({
                error: 'Failed to submit heatmap query'
            });
        }
    }
});

router.post('/from_utah', async (req: Request, res: Response) => {
    try {
        // Validate request body
        const { month, startDate, endDate } = utahBoundarySchema.parse(req.body);

        // Query to get all points from trucks that start in Utah and end outside
        const query = `
            WITH qualifying_trucks AS (
                SELECT DISTINCT route_id
                FROM (
                    SELECT
                        route_id,
                        FIRST_VALUE(location) OVER (PARTITION BY route_id ORDER BY timestamp) AS start_location,
                        FIRST_VALUE(location) OVER (PARTITION BY route_id ORDER BY timestamp DESC) AS end_location,
                        FIRST_VALUE(timestamp) OVER (PARTITION BY route_id ORDER BY timestamp) AS first_timestamp
                    FROM month_${month.toString().padStart(2, '0')}_routes
                ) tr
                WHERE ST_Within(tr.start_location::geometry, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326))
                  AND NOT ST_Within(tr.end_location::geometry, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326))
                  AND tr.first_timestamp BETWEEN EXTRACT(EPOCH FROM '${startDate}'::timestamp)::bigint 
                                        AND EXTRACT(EPOCH FROM '${endDate}'::timestamp)::bigint
            )
            SELECT 
                r.route_id,
                r.timestamp,
                ST_Y(r.location::geometry) as latitude,
                ST_X(r.location::geometry) as longitude
            FROM month_${month.toString().padStart(2, '0')}_routes r
            INNER JOIN qualifying_trucks qt ON r.route_id = qt.route_id
            ORDER BY r.route_id, r.timestamp
            LIMIT 1000;
        `;

        const job = await queueService.submitQuery(query, {
            type: 'regular'
        });

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
            console.error('Error submitting from Utah query:', error);
            res.status(500).json({
                error: 'Failed to submit query'
            });
        }
    }
});

router.post('/to_utah', async (req: Request, res: Response) => {
    try {
        // Validate request body
        // Query to get all points from trucks that start outside Utah and end inside
        const { month, startDate, endDate } = utahBoundarySchema.parse(req.body);
        const query = `
            WITH qualifying_trucks AS (
                SELECT DISTINCT route_id
                FROM (
                    SELECT
                        route_id,
                        FIRST_VALUE(location) OVER (PARTITION BY route_id ORDER BY timestamp) AS start_location,
                        FIRST_VALUE(location) OVER (PARTITION BY route_id ORDER BY timestamp DESC) AS end_location,
                        FIRST_VALUE(timestamp) OVER (PARTITION BY route_id ORDER BY timestamp) AS first_timestamp
                    FROM month_${month.toString().padStart(2, '0')}_routes
                ) tr
                WHERE NOT ST_Within(tr.start_location::geometry, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326))
                  AND ST_Within(tr.end_location::geometry, ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326))
                  AND tr.first_timestamp BETWEEN EXTRACT(EPOCH FROM '${startDate}'::timestamp)::bigint 
                                        AND EXTRACT(EPOCH FROM '${endDate}'::timestamp)::bigint
            )
            SELECT 
                r.route_id,
                r.timestamp,
                ST_Y(r.location::geometry) as latitude,
                ST_X(r.location::geometry) as longitude
            FROM month_${month.toString().padStart(2, '0')}_routes r
            INNER JOIN qualifying_trucks qt ON r.route_id = qt.route_id
            ORDER BY r.route_id, r.timestamp
            LIMIT 1000;
        `;

        const job = await queueService.submitQuery(query, {
            type: 'regular'
        });

        res.json({
            jobId: job.id,
            status: job.status,
            message: 'Query submitted successfully'
        });
    } catch (error) {
        if (error instanceof z.ZodError) {
            res.status(400).json({
                error: 'Invalid request format',
                details: error.errors,
            });
        } else {
            console.error('Error submitting to Utah query:', error);
            res.status(500).json({
                error: 'Failed to submit query'
            });
        }
    }
});

export default router; 