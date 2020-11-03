/*
 First, create a two-column table composed of urn & urnParts
 This table will only contain distinct URNs which only exist in metadata_aspect but not in metadata_index
 urn: Actual dataset URN
 urnParts: Stripped dataset URN which only contains fields of the dataset URN
 Example dataset_urn_parts table:
+-----------------------------------------------------+------------------------------------+
| urn                                                 | urn_parts                          |
+-----------------------------------------------------+------------------------------------+
| urn:li:dataset:(urn:li:dataPlatform:ambry,aaa.2,EI) | urn:li:dataPlatform:ambry,aaa.2,EI |
+-----------------------------------------------------+------------------------------------+
 */
CREATE TEMPORARY TABLE dataset_urn_parts
  SELECT urn,
         substring(urn, 17, urnLength - 17) AS urn_parts # len("urn:li:dataset:(") = 16 and indexing in SQL starts at 1
  FROM   (SELECT DISTINCT urn,
                          length(urn) AS urnLength
          FROM   metadata_aspect
          WHERE  urn NOT IN (SELECT DISTINCT urn
                             FROM   metadata_index
                             WHERE  aspect="com.linkedin.common.urn.DatasetUrn")
                 AND urn RLIKE "^urn:li:dataset:\\(urn:li:dataPlatform:[^:,]+,[^:,]+,[^:,]+\\)$"
                 AND version=0)
         AS
         dataset_urn_parts;

/*
 Parse platform field of the dataset URN and insert these as rows into the index table
 */
INSERT INTO metadata_index
            (urn,
             aspect,
             path,
             stringVal)
SELECT urn,
       "com.linkedin.common.urn.DatasetUrn",
       "/platform"                        AS path,
       substring_index(urn_parts, ',', 1) AS stringVal
FROM   dataset_urn_parts;

/*
 Parse datasetName field of the dataset URN and insert these as rows into the index table
 */
INSERT INTO metadata_index
            (urn,
             aspect,
             path,
             stringVal)
SELECT urn,
       "com.linkedin.common.urn.DatasetUrn",
       "/datasetName"                                               AS path,
       substring_index(substring_index(urn_parts, ',', 2), ',', -1) AS stringVal
FROM   dataset_urn_parts;

/*
 Parse origin field of the dataset URN and insert these as rows into the index table
 */
INSERT INTO metadata_index
            (urn,
             aspect,
             path,
             stringVal)
SELECT urn,
       "com.linkedin.common.urn.DatasetUrn",
       "/origin"                           AS path,
       substring_index(urn_parts, ',', -1) AS stringVal
FROM   dataset_urn_parts;

/*
 Parse platformName field of the data platform URN and insert these as rows into the index table
 */
INSERT INTO metadata_index
            (urn,
             aspect,
             path,
             stringVal)
SELECT urn,
       "com.linkedin.common.urn.DatasetUrn",
       "/platform/platformName"                                     AS path,
       substring_index(substring_index(urn_parts, ',', 1), ':', -1) AS stringVal
FROM   dataset_urn_parts;

/*
 Get rid of dataset_urn_parts table
 */
DROP TEMPORARY TABLE dataset_urn_parts;