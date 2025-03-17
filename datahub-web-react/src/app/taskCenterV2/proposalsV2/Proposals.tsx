import { colors, Button as TabButton } from '@components';
import { Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { ActionRequest, AssigneeType, CorpGroup } from '../../../types.generated';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';
import { ProposalList } from './ProposalList';
import { ActionRequestGroup, MY_PROPOSALS_GROUP_NAME, PERSONAL_ACTION_REQUESTS_GROUP_NAME } from './utils';

const StyledButtonGroup = styled(Button.Group)`
    margin: 8px 16px;
`;

const ActiveGroupTabViewContainer = styled.div`
    height: calc(100% - 70px);
    margin: 20px;
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
            {
                name: MY_PROPOSALS_GROUP_NAME,
                displayName: MY_PROPOSALS_GROUP_NAME,
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

    const filteredActionRequestGroups = actionRequestGroups.filter((group) => group.name === actionRequestGroupName);
    const activeActionRequestGroup = filteredActionRequestGroups.length > 0 && filteredActionRequestGroups[0];
    const activeActionRequestGroupTabView = activeActionRequestGroup && (
        <ActiveGroupTabViewContainer>
            <ProposalList
                assignee={activeActionRequestGroup.assignee}
                groupName={activeActionRequestGroup.name}
                userUrn={authenticatedUser?.corpUser.urn}
                showFilters
                onProposalClick={onProposalClick}
            />
        </ActiveGroupTabViewContainer>
    );

    return (
        <ProposalsContainer>
            <StyledButtonGroup>
                {actionRequestGroups.map((group) => (
                    <StyledTabButton
                        variant="text"
                        onClick={() => setActionRequestGroupName(group.name)}
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
