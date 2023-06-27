import React, { useEffect, useState } from 'react';
import { Button } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import { SidebarHeader } from '../SidebarHeader';
import { setupJiraIssueCollector } from './SetupJiraIssueCollector';
import { useEntityData } from '../../../../EntityContext';

interface Props {
    readOnly?: boolean;
}

export const SidebarAccessRequestSection = ({ readOnly }: Props) => {
    const { entityData } = useEntityData();
    const entityUrn = entityData?.urn || '';

    const [loading, setLoading] = useState(true);

    useEffect(() => {
        if (entityUrn) {
            setLoading(false);
        }
    }, [entityUrn]);

    useEffect(() => {
        if (!loading) {
            const jiraScriptUrl =
                'https://sharphealthcare.atlassian.net/s/d41d8cd98f00b204e9800998ecf8427e-T/1jmxwi/b/8/c95134bc67d3a521bb3f4331beb9b804/_/download/batch/com.atlassian.jira.collector.plugin.jira-issue-collector-plugin:issuecollector/com.atlassian.jira.collector.plugin.jira-issue-collector-plugin:issuecollector.js?locale=en-US&collectorId=2dd71ebc';

            const script = document.createElement('script');
            script.src = jiraScriptUrl;
            script.async = true;
            script.onload = () => {
                setupJiraIssueCollector('sidebar-access-request-button', entityUrn);
            };
            document.body.appendChild(script);
            return () => {
                document.body.removeChild(script);
            };
        }
        return () => {};
    }, [entityUrn, loading]);
    return (
        <div>
            <SidebarHeader title="Access Request" />
            <div>
                {!readOnly && (
                    <Button id="sidebar-access-request-button" type="default">
                        <EditOutlined /> Request Access
                    </Button>
                )}
            </div>
        </div>
    );
};

export default SidebarAccessRequestSection;
