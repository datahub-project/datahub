import React, { useMemo, useState } from 'react';
import { Button, List, message, Modal, Pagination, Typography } from 'antd';
import styled from 'styled-components';

import { SearchablePage } from '../search/SearchablePage';
import PolicyBuilderModal from './PolicyBuilderModal';
import { Policy, PolicyUpdateInput, PolicyState } from '../../types.generated';
import PolicyListItem from './PolicyListItem';
import PolicyDetailsModal from './PolicyDetailsModal';
import {
    useCreatePolicyMutation,
    useDeletePolicyMutation,
    useListPoliciesQuery,
    useUpdatePolicyMutation,
} from '../../graphql/policy.generated';
import { Message } from '../shared/Message';
import { EMPTY_POLICY } from './policyUtils';

const PoliciesContainer = styled.div`
    padding: 40px;
    padding-left: 120px;
    padding-right: 120px;
`;

const PoliciesTitle = styled(Typography.Title)`
    && {
        margin-bottom: 24px;
    }
`;

const NewPolicyButton = styled(Button)`
    margin-bottom: 16px;
`;

const PolicyList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        margin-top: 12px;
        padding: 16px 32px;
        box-shadow: ${(props) => props.theme.styles['box-shadow']};
    }
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const DEFAULT_PAGE_SIZE = 10;

const toPolicyInput = (policy: Omit<Policy, 'urn'>): PolicyUpdateInput => {
    let policyInput: PolicyUpdateInput = {
        type: policy.type,
        name: policy.name,
        state: policy.state,
        description: policy.description,
        privileges: policy.privileges,
        actors: {
            users: policy.actors.users,
            groups: policy.actors.groups,
            allUsers: policy.actors.allUsers,
            allGroups: policy.actors.allGroups,
            resourceOwners: policy.actors.resourceOwners,
        },
    };
    if (policy.resources !== null && policy.resources !== undefined) {
        // Add the resource filters.
        policyInput = {
            ...policyInput,
            resources: {
                type: policy.resources.type,
                resources: policy.resources.resources,
                allResources: policy.resources.allResources,
            },
        };
    }
    return policyInput;
};

// TODO: Cleanup the styling.
export const PoliciesPage = () => {
    const [page, setPage] = useState(1);

    // Controls whether the editing and details view modals are active.
    const [showPolicyBuilderModal, setShowPolicyBuilderModal] = useState(false);
    const [showViewPolicyModal, setShowViewPolicyModal] = useState(false);

    // Focused policy represents a policy being actively viewed, edited, created via a popup modal.
    const [focusPolicyUrn, setFocusPolicyUrn] = useState<undefined | string>(undefined);
    const [focusPolicy, setFocusPolicy] = useState<Omit<Policy, 'urn'>>(EMPTY_POLICY);

    // Policy list paging.
    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const {
        loading: policiesLoading,
        error: policiesError,
        data: policiesData,
    } = useListPoliciesQuery({
        fetchPolicy: 'no-cache',
        variables: { input: { start, count: pageSize } },
    });

    const listPoliciesQuery = 'listPolicies';

    // Any time a policy is removed, edited, or created, refetch the list.
    const [createPolicy, { error: createPolicyError }] = useCreatePolicyMutation({
        refetchQueries: () => [listPoliciesQuery],
    });

    const [updatePolicy, { error: updatePolicyError }] = useUpdatePolicyMutation({
        refetchQueries: () => [listPoliciesQuery],
    });

    const [deletePolicy, { error: deletePolicyError }] = useDeletePolicyMutation({
        refetchQueries: () => [listPoliciesQuery],
    });

    const updateError = createPolicyError || updatePolicyError || deletePolicyError;

    const totalPolicies = policiesData?.listPolicies?.total || 0;
    const policies = useMemo(() => policiesData?.listPolicies?.policies || [], [policiesData]);

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    const onClickNewPolicy = () => {
        setFocusPolicyUrn(undefined);
        setFocusPolicy(EMPTY_POLICY);
        setShowPolicyBuilderModal(true);
    };

    const onClosePolicyBuilder = () => {
        setFocusPolicyUrn(undefined);
        setFocusPolicy(EMPTY_POLICY);
        setShowPolicyBuilderModal(false);
    };

    const onViewPolicy = (policy: Policy) => {
        setShowViewPolicyModal(true);
        setFocusPolicyUrn(policy.urn);
        setFocusPolicy({ ...policy });
    };

    const onCancelViewPolicy = () => {
        setShowViewPolicyModal(false);
        setFocusPolicy(EMPTY_POLICY);
        setFocusPolicyUrn(undefined);
    };

    const onRemovePolicy = () => {
        Modal.confirm({
            title: `Delete ${focusPolicy.name}`,
            content: `Are you sure you want to remove policy?`,
            onOk() {
                deletePolicy({ variables: { urn: focusPolicyUrn as string } }); // There must be a focus policy urn.
                onCancelViewPolicy();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const onEditPolicy = () => {
        setShowViewPolicyModal(false);
        setShowPolicyBuilderModal(true);
    };

    const onToggleActive = () => {
        const newPolicy = {
            ...focusPolicy,
            state: focusPolicy?.state === PolicyState.Active ? PolicyState.Inactive : PolicyState.Active,
        };
        updatePolicy({
            variables: {
                urn: focusPolicyUrn as string, // There must be a focus policy urn.
                input: toPolicyInput(newPolicy),
            },
        });
        setShowViewPolicyModal(false);
    };

    const onSavePolicy = (savePolicy: Omit<Policy, 'urn'>) => {
        if (focusPolicyUrn) {
            // If there's an URN associated with the focused policy, then we are editing an existing policy.
            updatePolicy({ variables: { urn: focusPolicyUrn, input: toPolicyInput(savePolicy) } });
        } else {
            // If there's no URN associated with the focused policy, then we are creating.
            createPolicy({ variables: { input: toPolicyInput(savePolicy) } });
        }
        message.success('Successfully saved policy.');
        onClosePolicyBuilder();
    };

    return (
        <SearchablePage>
            {policiesLoading && <Message type="loading" content="Loading policies..." />}
            {policiesError && message.error('Failed to load policies :(')}
            {updateError && message.error('Failed to update the Policy :(')}
            <PoliciesContainer>
                <PoliciesTitle level={2}>Manage Policies</PoliciesTitle>
                <NewPolicyButton onClick={onClickNewPolicy} data-testid="add-policy-button">
                    + New Policy
                </NewPolicyButton>
                <PolicyList
                    bordered
                    dataSource={policies}
                    renderItem={(item: unknown) => (
                        <PolicyListItem policy={item as Policy} onView={() => onViewPolicy(item as Policy)} />
                    )}
                />
                <PaginationContainer>
                    <Pagination
                        style={{ margin: 40 }}
                        current={page}
                        pageSize={pageSize}
                        total={totalPolicies}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </PaginationContainer>
            </PoliciesContainer>
            {showPolicyBuilderModal && (
                <PolicyBuilderModal
                    policy={focusPolicy || EMPTY_POLICY}
                    setPolicy={setFocusPolicy}
                    visible={showPolicyBuilderModal}
                    onClose={onClosePolicyBuilder}
                    onSave={onSavePolicy}
                />
            )}
            {showViewPolicyModal && (
                <PolicyDetailsModal
                    policy={focusPolicy}
                    visible={showViewPolicyModal}
                    onEdit={onEditPolicy}
                    onClose={onCancelViewPolicy}
                    onRemove={onRemovePolicy}
                    onToggleActive={onToggleActive}
                />
            )}
        </SearchablePage>
    );
};
