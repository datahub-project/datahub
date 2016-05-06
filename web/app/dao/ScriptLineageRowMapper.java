package dao;

import models.ScriptLineage;
import org.springframework.jdbc.core.RowMapper;

import java.sql.*;

public class ScriptLineageRowMapper implements RowMapper<ScriptLineage>
{
    public static String SOURCE_TARGET_TYPE_COLUMN = "source_target_type";
    public static String ABSTRACTED_OBJECT_NAME_COLUMN = "abstracted_object_name";
    public static String OPERATION_TYPE_COLUMN = "operation";
    public static String STORAGE_TYPE_COLUMN = "storage_type";

    @Override
    public ScriptLineage mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        String sourceTargetType = rs.getString(SOURCE_TARGET_TYPE_COLUMN);
        String abstractedObjectName = rs.getString(ABSTRACTED_OBJECT_NAME_COLUMN);
        String operation = rs.getString(OPERATION_TYPE_COLUMN);
        String storageType = rs.getString(STORAGE_TYPE_COLUMN);

        ScriptLineage lineage = new ScriptLineage();
        lineage.sourceTargetType = sourceTargetType;
        lineage.abstractedObjectName = abstractedObjectName;
        lineage.storageType = storageType;
        lineage.operation = operation;
        return lineage;
    }
}