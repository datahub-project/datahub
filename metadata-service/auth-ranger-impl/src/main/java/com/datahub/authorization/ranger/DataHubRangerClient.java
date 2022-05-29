package com.datahub.authorization.ranger;

import java.util.Set;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;


public interface DataHubRangerClient {
    public void init();
    public Set<String> getUserGroups(String userIdentifier);
    public Set<String> getUserRoles(String userIdentifier);
    public RangerAccessResult isAccessAllowed(RangerAccessRequest rangerAccessRequest);
}
