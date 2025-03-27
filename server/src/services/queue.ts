import amqp from 'amqplib';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();
const QUEUE_NAME = 'query_queue';

export class QueueService {
    private connection: amqp.ChannelModel | null = null;
    private channel: amqp.Channel | null = null;

    async connect() {
        try {
            this.connection = await amqp.connect(process.env.RABBITMQ_URL || 'amqp://localhost');
            this.channel = await this.connection.createChannel();
            await this.channel.assertQueue(QUEUE_NAME, { durable: true });
        } catch (error) {
            console.error('Failed to connect to RabbitMQ:', error);
            throw error;
        }
    }

    async submitQuery(query: string) {
        if (!this.channel) {
            throw new Error('Queue not connected');
        }

        // Create a query job record
        const queryJob = await prisma.queryJob.create({
            data: {
                query,
                status: 'pending'
            }
        });

        // Send the job to the queue
        await this.channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify({
            jobId: queryJob.id,
            query
        })));

        return queryJob;
    }

    async getQueryStatus(jobId: string) {
        return prisma.queryJob.findUnique({
            where: { id: jobId }
        });
    }

    async close() {
        if (this.channel) {
            await this.channel.close();
            this.channel = null;
        }
        if (this.connection) {
            await this.connection.close();
            this.connection = null;
        }
    }
} 