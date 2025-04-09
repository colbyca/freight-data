import json
import os
import pika
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from handlers import RegularQueryHandler, HeatmapHandler

load_dotenv()

class QueryWorker:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.engine = create_engine(os.getenv('DATABASE_URL'))
        self.queue_name = 'query_queue'
        
        # Initialize handlers with engine
        self.handlers = {
            'regular': RegularQueryHandler(),
            'heatmap': HeatmapHandler()
        }
        
        # Set engine for each handler
        for handler in self.handlers.values():
            handler.engine = self.engine
            
        self.connect()

    def connect(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=os.getenv('RABBITMQ_HOST', 'localhost'))
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.process_query)
        except Exception as e:
            print(f"Failed to connect to RabbitMQ: {e}")
            raise

    def update_job_status(self, job_id: str, status: str, result=None, error=None):
        with self.engine.connect() as conn:
            query = text("""
                UPDATE "QueryJob"
                SET status = :status,
                    result = :result,
                    error = :error,
                    "completedAt" = CASE WHEN :status IN ('completed', 'failed') THEN NOW() ELSE NULL END
                WHERE id = :job_id
            """)
            conn.execute(query, {
                "status": status,
                "result": json.dumps(result) if result else None,
                "error": error,
                "job_id": job_id
            })
            conn.commit()

    def process_query(self, ch, method, properties, body):
        try:
            data = json.loads(body)
            job_id = data['jobId']
            query = data['query']
            job_type = data.get('type', 'regular')
            params = data.get('params', {})

            self.update_job_status(job_id, 'processing')

            # Get the appropriate handler
            handler = self.handlers.get(job_type)
            if not handler:
                raise ValueError(f"Unknown job type: {job_type}")

            # Execute the handler
            result = handler.process(query, params)
            self.update_job_status(job_id, 'completed', result=result)

        except Exception as e:
            print(f"Error processing query: {e}")
            self.update_job_status(job_id, 'failed', error=str(e))

        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        print("Worker started. Waiting for messages...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.connection.close()

if __name__ == '__main__':
    worker = QueryWorker()
    worker.run() 