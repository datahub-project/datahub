import React, { useEffect } from 'react';
import { Button } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import { SidebarHeader } from '../SidebarHeader';
import { setupJiraIssueCollector } from './SetupJiraIssueCollector';

interface Props {
    readOnly?: boolean;
}

export const SidebarAccessRequestSection = ({ readOnly }: Props) => {
    useEffect(() => {
        const jiraScriptUrl = "jira_url";

        const script = document.createElement('script');
        script.src = jiraScriptUrl;
        script.async = true;
        script.onload = () => {
            setupJiraIssueCollector('sidebar-access-request-button');
        };
        document.body.appendChild(script);
        return () => {
            document.body.removeChild(script);
        };
    }, []);
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