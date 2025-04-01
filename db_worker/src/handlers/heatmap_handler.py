import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from collections import Counter

class HeatmapHandler:
    def process(self, query: str, params: dict):
        """Generate heatmap data from database query results"""
        try:
            # Use pandas to read the SQL query directly
            df = pd.read_sql_query(query, self.engine)

            if df.empty:
                return {
                    'total_points': 0,
                    'max_intensity': 0,
                    'heatmap_data': []
                }

            # Extract coordinates
            coordinates = df[['latitude', 'longitude']].values

            # Apply DBSCAN clustering with provided parameters
            db = DBSCAN(
                eps=params['eps'],
                min_samples=params['minSamples'],
                metric='haversine'
            ).fit(np.radians(coordinates))

            # Assign cluster labels
            df['cluster'] = db.labels_

            # Count stops per cluster
            cluster_counts = Counter(df['cluster'])
            max_count = max(cluster_counts.values()) if cluster_counts else 0

            # Compute intensity (normalized stop count)
            heatmap_data = []
            for cluster, count in cluster_counts.items():
                if cluster == -1:  # Ignore noise points
                    continue
                cluster_points = df[df['cluster'] == cluster]
                avg_lat = cluster_points['latitude'].mean()
                avg_lon = cluster_points['longitude'].mean()
                avg_duration = cluster_points['duration_minutes'].mean()
                
                intensity = count / max_count if max_count > 0 else 0
                heatmap_data.append({
                    'latitude': float(avg_lat),
                    'longitude': float(avg_lon),
                    'intensity': float(intensity),
                    'count': int(count),
                    'avg_duration_minutes': float(avg_duration)
                })

            return {
                'total_points': len(df),
                'max_intensity': float(max_count),
                'heatmap_data': heatmap_data
            }

        except Exception as e:
            print(f"Error generating heatmap: {str(e)}")
            raise 