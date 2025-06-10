import { Button as TabButton, colors } from '@components';
import { Button } from 'antd';
import React, { useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import { ProposalList } from '@app/taskCenterV2/proposalsV2/ProposalList';
import {
    ActionRequestGroup,
    MY_PROPOSALS_GROUP_NAME,
    PERSONAL_ACTION_REQUESTS_GROUP_NAME,
} from '@app/taskCenterV2/proposalsV2/utils';
import { ACTION_REQUEST_DISPLAY_FACETS } from '@app/taskCenterV2/utils/constants';
import { useGetAuthenticatedUser } from '@app/useGetAuthenticatedUser';

import { ActionRequest, ActionRequestStatus, AssigneeType, FilterOperator } from '@types';

const StyledButtonGroup = styled(Button.Group)`
    margin: 8px 16px;
`;

const ActiveGroupTabViewContainer = styled.div`
    height: calc(100% - 58px);
    margin: 20px;
    margin-top: 8px;
`;

const ProposalsContainer = styled.div`
    height: 100%;
`;

const StyledTabButton = styled(TabButton)<{ $isSelected: boolean }>`
    color: ${(props) => (props.$isSelected ? colors.violet : colors.gray[600])};
    background-color: ${(props) => (props.$isSelected ? colors.gray[1000] : 'transparent')};
    font-weight: ${(props) => (props.$isSelected ? '600' : '400')};
    padding: 10px;

    &:focus {
        background-color: ${(props) => (props.$isSelected ? colors.gray[1000] : 'transparent')};
        box-shadow: none;
    }
`;

type Props = {
    onProposalClick?: (record: ActionRequest) => void;
};

export const Proposals = ({ onProposalClick }: Props) => {
    const location = useLocation();
    const history = useHistory();

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
     * The second is for the "My Proposals" or outbox, where action requests
     * having the authenticated user urn as the creator are displayed.
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
                initialFilters: [
                    {
                        field: 'status',
                        condition: FilterOperator.Equal,
                        values: [ActionRequestStatus.Pending],
                        negated: false,
                    },
                ],
            },
            {
                name: MY_PROPOSALS_GROUP_NAME,
                displayName: MY_PROPOSALS_GROUP_NAME,
                createdBy: {
                    urn: authenticatedUser?.corpUser?.urn,
                },
                defaultFilters: [
                    {
                        field: 'createdBy',
                        condition: FilterOperator.Equal,
                        values: [authenticatedUser?.corpUser?.urn],
                        negated: false,
                    },
                ],
            },
        ]) ||
        [];

    const handleGroupSwitch = (group) => {
        setActionRequestGroupName(group.name);
        // Clear the query params of the other groups
        history.push(location.pathname);
    };

    const filteredActionRequestGroups = actionRequestGroups.filter((group) => group.name === actionRequestGroupName);
    const activeActionRequestGroup = filteredActionRequestGroups.length > 0 && filteredActionRequestGroups[0];
    const activeActionRequestGroupTabView = activeActionRequestGroup && (
        <ActiveGroupTabViewContainer>
            <ProposalList
                assignee={activeActionRequestGroup.assignee}
                showFilters
                onProposalClick={onProposalClick}
                useUrlParams
                defaultFilters={activeActionRequestGroup.defaultFilters}
                initialFilters={activeActionRequestGroup.initialFilters}
                getAllActionRequests={activeActionRequestGroup.name === MY_PROPOSALS_GROUP_NAME}
                filterFacets={
                    activeActionRequestGroup.name === MY_PROPOSALS_GROUP_NAME
                        ? ['type', 'status']
                        : ACTION_REQUEST_DISPLAY_FACETS
                }
                enableSelection={activeActionRequestGroup.name !== MY_PROPOSALS_GROUP_NAME}
                showPendingView={activeActionRequestGroup.name === MY_PROPOSALS_GROUP_NAME}
                showAssignee={activeActionRequestGroup.name === MY_PROPOSALS_GROUP_NAME}
                key={activeActionRequestGroup.name}
            />
        </ActiveGroupTabViewContainer>
    );

    return (
        <ProposalsContainer>
            <StyledButtonGroup>
                {actionRequestGroups.map((group) => (
                    <StyledTabButton
                        variant="text"
                        onClick={() => handleGroupSwitch(group)}
                        $isSelected={
                            (activeActionRequestGroup && activeActionRequestGroup.name === group.name) ?? false
                        }
                    >
                        {group.displayName}
                    </StyledTabButton>
                ))}
            </StyledButtonGroup>
            {activeActionRequestGroupTabView}
        </ProposalsContainer>
    );
};
