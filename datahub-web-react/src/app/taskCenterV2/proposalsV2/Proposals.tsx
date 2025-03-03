import React, { useState } from 'react';

import { Tabs } from 'antd';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { ActionRequestAssignee, AssigneeType, CorpGroup } from '../../../types.generated';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';
import { ProposalList } from './ProposalList';

const StyledTabs = styled(Tabs)`
    &&& .ant-tabs-nav {
        margin-bottom: 0;
        padding-left: 28px;
    }
    &&& .ant-tabs-nav-list .ant-tabs-ink-bar {
        background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
    &&& .ant-tabs-tab-active .ant-tabs-tab-btn {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }

    &&& .ant-tabs-tab-active .ant-tabs-tab-btn,
    &&& .ant-tabs-tab .ant-tabs-tab-btn {
        padding: 0 20px;
    }

    &&& .ant-tabs-tab + .ant-tabs-tab {
        margin: 0px;
    }
`;

const Tab = styled(Tabs.TabPane)`
    font-size: 12px;
    line-height: 22px;
`;

const PERSONAL_ACTION_REQUESTS_GROUP_NAME = 'Personal';

type ActionRequestGroup = {
    name: string;
    displayName: string;
    assignee: ActionRequestAssignee;
};

export const Proposals = () => {
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
                displayName: PERSONAL_ACTION_REQUESTS_GROUP_NAME,
                assignee: {
                    type: AssigneeType.User,
                    urn: authenticatedUser?.corpUser?.urn,
                },
            },
            ...(authenticatedUser?.corpUser.groups?.relationships?.map((rel) => {
                const group = rel.entity as CorpGroup;
                return {
                    name: group.name,
                    displayName: group.properties?.displayName || group.name,
                    assignee: {
                        type: AssigneeType.Group,
                        urn: group.urn,
                    },
                };
            }) || []),
        ]) ||
        [];

    const onClickTab = (newRequestGroup: string) => setActionRequestGroupName(newRequestGroup);

    const filteredActionRequestGroups = actionRequestGroups.filter((group) => group.name === actionRequestGroupName);
    const activeActionRequestGroup = filteredActionRequestGroups.length > 0 && filteredActionRequestGroups[0];
    const activeActionRequestGroupTabView = activeActionRequestGroup && (
        <ProposalList assignee={activeActionRequestGroup.assignee} />
    );

    return (
        <>
            <StyledTabs activeKey={actionRequestGroupName} onTabClick={(tab: string) => onClickTab(tab)}>
                {actionRequestGroups.map((group) => (
                    <Tab key={group.name} tab={group.displayName} />
                ))}
            </StyledTabs>
            {activeActionRequestGroupTabView}
        </>
    );
};
