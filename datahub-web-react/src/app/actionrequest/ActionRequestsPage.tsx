import { Tabs, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { ActionRequestAssignee, AssigneeType, CorpGroup } from '../../types.generated';
import { SearchablePage } from '../search/SearchablePage';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { ActionRequestsGroupTab } from './ActionRequestsGroupTab';

const PageContainer = styled.div`
    padding-top: 40px;
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 24px;
    }
`;

const PageTitle = styled(Typography.Title)`
    && {
        margin-bottom: 24px;
    }
`;

const StyledTabs = styled(Tabs)`
    &&& .ant-tabs-nav {
        margin-bottom: 0;
        padding-left: 28px;
    }
`;

const Tab = styled(Tabs.TabPane)`
    font-size: 14px;
    line-height: 22px;
`;

const PERSONAL_ACTION_REQUESTS_GROUP_NAME = 'Personal';

type ActionRequestGroup = {
    name: string;
    assignee: ActionRequestAssignee;
};

export const ActionRequestsPage = () => {
    /**
     * Determines which view should be visible: pending or completed requests.
     */
    const [actionRequestGroupName, setActionRequestGroupName] = useState<string>(PERSONAL_ACTION_REQUESTS_GROUP_NAME);
    /**
     * Get the authenticated user + groups to render action request lists
     */
    const authenticatedUser = useGetAuthenticatedUser();

    /**
     * The set of groups to show as distinct tabs.
     *
     * The first is for the "personal" inbox, where action requests
     * having the authenticated user urn as the assignee are displayed.
     *
     * The subsequent are "group" inboxes, where action requests
     * directed to a specific group are displayed.
     */
    const actionRequestGroups: Array<ActionRequestGroup> =
        (authenticatedUser && [
            {
                name: PERSONAL_ACTION_REQUESTS_GROUP_NAME,
                assignee: {
                    type: AssigneeType.User,
                    urn: authenticatedUser?.corpUser.urn,
                },
            },
            ...(authenticatedUser?.corpUser.groups?.relationships?.map((rel) => {
                const group = rel.entity as CorpGroup;
                return {
                    name: group.name,
                    assignee: {
                        type: AssigneeType.Group,
                        urn: group.urn,
                    },
                };
            }) || []),
        ]) ||
        [];

    const onClickTab = (newRequestGroup: string) => {
        setActionRequestGroupName(newRequestGroup);
    };

    const filteredActionRequestGroups = actionRequestGroups.filter((group) => group.name === actionRequestGroupName);
    const activeActionRequestGroup = filteredActionRequestGroups.length > 0 && filteredActionRequestGroups[0];
    const activeActionRequestGroupTabView = activeActionRequestGroup && (
        <ActionRequestsGroupTab assignee={activeActionRequestGroup.assignee} />
    );

    return (
        <SearchablePage>
            <PageContainer>
                <PageHeaderContainer>
                    <PageTitle level={2}>My Requests</PageTitle>
                    <Typography.Paragraph type="secondary">
                        The inbox for requests assigned to you or a group you are a part of.
                    </Typography.Paragraph>
                </PageHeaderContainer>
                <StyledTabs
                    activeKey={actionRequestGroupName}
                    size="large"
                    onTabClick={(tab: string) => onClickTab(tab)}
                >
                    {actionRequestGroups.map((group) => (
                        <Tab key={group.name} tab={group.name} />
                    ))}
                </StyledTabs>
                {activeActionRequestGroupTabView}
            </PageContainer>
        </SearchablePage>
    );
};
