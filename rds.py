import boto3
import datetime


class Rds(object):

    def __init__(self, region) -> None:
        super().__init__()
        self.rds = boto3.client('rds', region)

    def cleanup_snapshot(self):
        self._cleanup_snapshot_instance()
        self._cleanup_snapshots_clusters()

    def cleanup_instances(self):
        clusters = self.rds.describe_db_clusters()
        for cluster in clusters['DBClusters']:
            self._cleanup_cluster(cluster)
        instances = self.rds.describe_db_instances()
        for instance in instances['DBInstances']:
            self._cleanup_instances(instance)


    def _stop_cluster(self, identifier):
        self.rds.stop_db_cluster(DBClusterIdentifier = identifier)

    def _stop_instance(self, identifier):
        self.rds.stop_db_instance(DBInstanceIdentifier = identifier)

    def _delete_instance(self, identifier):
        self.rds.describe_db_instances(DBInstanceIdentifier = identifier)

    def _delete_cluster(self, identifier):
        self.rds.describe_db_clusters(DBClusterIdentifier = identifier)

    def _delete_instance_snapshot(self, identifier):
        self.rds.delete_db_snapshot(DBSnapshotIdentifier = identifier)

    def _delete_cluster_snapshot(self, identifier):
        self.rds.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier = identifier)

    @staticmethod
    def _can_delete_instance(tags):
        if any('user' in tag for tag in tags):
            return False

    @staticmethod
    def _can_stop_instance(tags):
        for tag in tags:
            if tag["Key"].lower() == 'excludepower' and tag['Value'].lower() == 'true':
                return False
        return True

    @staticmethod
    def _can_delete_snapshot(tags):
        if tags is not None:
            for tag in tags:
                if tag['Key'].lower() == 'retain' and tag['Value'].lower()== 'true':
                    return False
            return True

    def _cleanup_instance(self, instance):
        identifier = instance['DBInstanceIdentifier']
        tags = instance['TagList']
        if self._can_delete_instance(tags):
            self._delete_instance(identifier)
        else:
            if self._can_stop_instance(tags) and instance['DBInstanceStatus'] == 'available':
                try:
                    self._stop_instance(identifier)
                except Exception as e:
                    print(str(e))

    