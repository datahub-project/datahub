SELECT a.audit_report_epoch, a.hdfs_name, a.dataset_path, a.owner_urns, a.rank FROM
  ( SELECT cast(`timestamp` / 1000 as bigint) as audit_report_epoch,
          metadata['ClusterIdentifier'] as hdfs_name,
          metadata['DatasetPath'] as dataset_path,
          metadata['OwnerURNs'] as owner_urns,
          RANK() OVER (PARTITION BY metadata['ClusterIdentifier'],
                      metadata['JobId'] ORDER BY metadata['ExecId'] DESC) as rank
   FROM service.GobblinTrackingEvent_audit
   WHERE datepartition >= from_unixtime(unix_timestamp() - 3*24*3600, 'yyyy-MM-dd')
         and namespace = 'idpc.auditor'
         and `name` in ('DaliLimitedRetentionAuditor', 'DaliAutoPurgeAuditor')
  ) as a
where a.rank = 1 order by a.hdfs_name, a.dataset_path