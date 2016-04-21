/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package models;

import java.util.List;

public class LineageNode {

    public int id;
    public String node_type;
    public String abstracted_path;
    public String storage_type;
    public String urn;
    public String job_type;
    public String cluster;
    public String project_name;
    public String job_path;
    public String job_name;
    public String script_name;
    public String script_path;
    public String job_start_time;
    public String job_end_time;
    public Long job_start_unix_time;
    public Long job_end_unix_time;
    public int level;
    public String git_location;
    public List<String> _sort_list;
    public String source_target_type;
    public Long exec_id;
    public Long job_id;
    public Long record_count;
    public int application_id;
    public String format_mask;
    public String partition_type;
    public String operation;
    public String partition_start;
    public String partition_end;
    public String full_object_name;
    public String pre_jobs;
    public String post_jobs;
}
