import datetime
import decimal
from sqlalchemy import text

class RegularQueryHandler:
    def process(self, query: str, params: dict = None):
        """Process a regular SQL query and return formatted results"""
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            rows = []
            for row in result:
                processed_row = {}
                for key, value in row._mapping.items():
                    if isinstance(value, decimal.Decimal):
                        processed_row[key] = float(value)
                    elif isinstance(value, (datetime.date, datetime.datetime)):
                        processed_row[key] = value.isoformat()
                    else:
                        processed_row[key] = str(value) if value is not None else None
                rows.append(processed_row)
            return rows 