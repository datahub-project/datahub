-- SPDX-License-Identifier: Apache-2.0
--
-- This file is unmodified from its original version developed by Acryl Data, Inc.,
-- and is now included as part of a repository maintained by the National Digital Twin Programme.
-- All support, maintenance and further development of this code is now the responsibility
-- of the National Digital Twin Programme.

BEGIN
  -- merge query with when matched and when not matched
  MERGE INTO target_table tgt
  USING (
    SELECT s1.id, s1.column1, s2.column2, s3.column3
    FROM source_table1 s1
    JOIN source_table2 s2 ON s1.id = s2.id 
    JOIN source_table3 s3 ON s1.id = s3.id
  ) src
  ON (tgt.id = src.id)
  WHEN MATCHED THEN
    UPDATE SET
      tgt.column1 = src.column1,
      tgt.column2 = src.column2,
      tgt.column3 = src.column3
  WHEN NOT MATCHED THEN
    INSERT (id, column1, column2, column3)
    VALUES (src.id, src.column1, src.column2, src.column3)
END
