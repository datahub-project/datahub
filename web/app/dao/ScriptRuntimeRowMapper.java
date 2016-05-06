package dao;

import models.ScriptRuntime;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;

import java.sql.*;

public class ScriptRuntimeRowMapper implements RowMapper<ScriptRuntime>
{
    public static String JOB_STARTED_COLUMN = "job_started";
    public static String ELAPSED_TIME_COLUMN = "elapsed_time";
    public static String FLOW_PATH_COLUMN = "flow_path";
    public static String JOB_NAME_COLUMN = "job_name";

    @Override
    public ScriptRuntime mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        String started = rs.getString(JOB_STARTED_COLUMN);
        Float elapsedTime = rs.getFloat(ELAPSED_TIME_COLUMN);
        String flowPath = rs.getString(FLOW_PATH_COLUMN);
        String jobName = rs.getString(JOB_NAME_COLUMN);

        ScriptRuntime runtime = new ScriptRuntime();
        runtime.jobStarted = started;
        runtime.elapsedTime = elapsedTime;
        if (StringUtils.isNotBlank(flowPath))
        {
            int index = flowPath.indexOf(':');
            if (index != -1)
            {
                flowPath = "/" + flowPath.substring(index+1);
            }
        }
        runtime.jobPath = flowPath + "/" + jobName;

        return runtime;
    }
}