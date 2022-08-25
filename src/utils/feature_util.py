"""
    Util module for features
"""
from typing import List
from feature.feature import Feature
from feature.featureCluster import FeatureCluster


def map_cluster_with_belonging_features(
    features: List[Feature], clusters: List[FeatureCluster]
):
    """Relate cluster(1) and features(N)

    :param features:
    :param clusters:
    :return:
    """
    for cluster in clusters:
        cluster_id = cluster.get_cluster_id()
        belonging_features = list(
            filter(lambda feature: feature.is_belonging_cluster(cluster_id), features)
        )
        cluster.link_belonging_features(belonging_features)
