import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from collections import Counter


def create_heatmap_csv(file_path, month, year):
    df = pd.read_csv(file_path, sep=';', names=['id', 'location', 'latitude', 'longitude', 'arrival_date', 'leaving_date', 'idk'])
    total_counts = len(df)
    # Convert latitude and longitude to floats
    df['latitude'] = pd.to_numeric(df['latitude'])
    df['longitude'] = pd.to_numeric(df['longitude'])

    # Extract necessary columns
    coordinates = df[['latitude', 'longitude']].values

    # Apply DBSCAN clustering
    eps = 0.001  # Adjust based on your data, eps values: 1=111km, .001=111m, .01=1.11km
    min_samples = 2  # Adjust based on desired minimum stop count
    db = DBSCAN(eps=eps, min_samples=min_samples, metric='haversine').fit(np.radians(coordinates))

    # Assign cluster labels
    df['cluster'] = db.labels_

    # Count stops per cluster
    cluster_counts = Counter(df['cluster'])
    max_count = max(cluster_counts.values())

    # Compute intensity (normalized stop count)
    heatmap_data = []
    for cluster, count in cluster_counts.items():
        if cluster == -1:  # Ignore noise points
            continue
        cluster_points = df[df['cluster'] == cluster]
        avg_lat = cluster_points['latitude'].mean()
        avg_lon = cluster_points['longitude'].mean()
        intensity = count / max_count
        heatmap_data.append((avg_lat, avg_lon, intensity))

    # Convert to DataFrame for visualization
    heatmap_df = pd.DataFrame(heatmap_data, columns=['latitude', 'longitude', 'intensity'])

    heatmap_df.to_csv(f"heatmap-{month}-{year}.csv", index=False)

    return total_counts, max_count
