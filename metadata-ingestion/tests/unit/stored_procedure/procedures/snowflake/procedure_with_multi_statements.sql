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
    SELECT id, column1 
    FROM source_table1
  ) src
  ON (tgt.id = src.id)
  WHEN MATCHED THEN
    UPDATE SET
      tgt.column1 = src.column1
  WHEN NOT MATCHED THEN
    INSERT (id, column1)
    VALUES (src.id, src.column1);

  INSERT INTO target_table_insert (id, column2)
  SELECT id, column2
  FROM source_table2;

  DELETE FROM target_table_delete
  WHERE id IN (
    SELECT id
    FROM source_table3
  )
END
