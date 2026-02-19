package datahub.hive.consumer.service.impl;

import com.google.gson.JsonObject;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.query.QueryLanguage;
import com.linkedin.query.QueryProperties;
import com.linkedin.query.QuerySource;
import com.linkedin.query.QueryStatement;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjectArray;
import com.linkedin.query.QuerySubjects;

import datahub.hive.consumer.config.Constants;
import datahub.hive.consumer.service.MCPTransformerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of MCPTransformerService for Query entities.
 */
@Service
@Slf4j
public class QueryMCPTransformerServiceImpl implements MCPTransformerService {

    @Value("${application.environment}")
    private String environment;

    @Value("${application.service.user}")
    private String serviceUser;

    @Override
    public List<DataTemplate<DataMap>> transformToMCP(JsonObject queryJsonObject) throws URISyntaxException {
        List<DataTemplate<DataMap>> aspects = new ArrayList<>();

        // Build querySubjects aspect
        aspects.add(buildQuerySubjectsAspect(queryJsonObject));

        // Build queryProperties aspect
        aspects.add(buildQueryPropertiesAspect(queryJsonObject));

        return aspects;
    }

    /**
     * Build the query subjects aspect for the query.
     */
    private DataTemplate<DataMap> buildQuerySubjectsAspect(JsonObject queryJsonObject) {
        QuerySubjects querySubjects = new QuerySubjects();
        List<QuerySubject> subjects = new ArrayList<>();
        
        // Add all output dataset as subjects
        String datasetName;
        if (queryJsonObject.has(Constants.OUTPUTS_KEY) && !queryJsonObject.getAsJsonArray(Constants.OUTPUTS_KEY).isEmpty()) {
            String fullName = queryJsonObject.getAsJsonArray(Constants.OUTPUTS_KEY).get(0).getAsString();
            String platformInstance = queryJsonObject.get(Constants.PLATFORM_INSTANCE_KEY).getAsString();
            datasetName = platformInstance + "." + fullName;
                
            // Create subject for the output dataset
            QuerySubject subject = new QuerySubject();
            
            // Create dataset URN
            DatasetUrn datasetUrn = new DatasetUrn(
                new DataPlatformUrn(Constants.PLATFORM_NAME),
                datasetName,
                FabricType.valueOf(environment)
            );
            
            subject.setEntity(datasetUrn);
            subjects.add(subject);
            
            querySubjects.setSubjects(new QuerySubjectArray(subjects));
        }
        
        return querySubjects;
    }

    /**
     * Build the query properties aspect for the query.
     */
    private DataTemplate<DataMap> buildQueryPropertiesAspect(JsonObject queryJsonObject) throws URISyntaxException {
        QueryProperties queryProperties = new QueryProperties();
        
        // Set the query statement
        QueryStatement statement = new QueryStatement();
        statement.setValue(queryJsonObject.get(Constants.QUERY_TEXT_KEY).getAsString());
        statement.setLanguage(QueryLanguage.SQL);
        queryProperties.setStatement(statement);
        
        // Set the created and last modified timestamps
        long timestamp = System.currentTimeMillis();
        
        AuditStamp created = new AuditStamp();
        created.setTime(timestamp);
        created.setActor(Urn.createFromString(Constants.CORP_USER_URN_PREFIX + serviceUser));
        queryProperties.setCreated(created);

        AuditStamp lastModified = new AuditStamp();
        lastModified.setTime(timestamp);
        lastModified.setActor(Urn.createFromString(Constants.CORP_USER_URN_PREFIX + serviceUser));
        queryProperties.setLastModified(lastModified);
        
        queryProperties.setSource(QuerySource.MANUAL);
        
        return queryProperties;
    }
}
