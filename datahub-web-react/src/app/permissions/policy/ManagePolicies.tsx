import React, { useEffect, useMemo, useState } from 'react';
import { Button, Empty, message, Pagination, Select, Tag } from 'antd';
import styled from 'styled-components/macro';
import * as QueryString from 'query-string';
import { DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import { useLocation } from 'react-router';
import PolicyBuilderModal from './PolicyBuilderModal';
import { AndFilterInput, Policy, PolicyState, FilterOperator } from '../../../types.generated';
import { useAppConfig } from '../../useAppConfig';
import PolicyDetailsModal from './PolicyDetailsModal';
import { useListPoliciesQuery } from '../../../graphql/policy.generated';
import { Message } from '../../shared/Message';
import { DEFAULT_PAGE_SIZE, EMPTY_POLICY } from './policyUtils';
import TabToolbar from '../../entity/shared/components/styled/TabToolbar';
import { StyledTable } from '../../entity/shared/components/styled/StyledTable';
import AvatarsGroup from '../AvatarsGroup';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { SearchBar } from '../../search/SearchBar';
import { scrollToTop } from '../../shared/searchUtils';
import analytics, { EventType } from '../../analytics';
import { POLICIES_CREATE_POLICY_ID, POLICIES_INTRO_ID } from '../../onboarding/config/PoliciesOnboardingConfig';
import { OnboardingTour } from '../../onboarding/OnboardingTour';
import { usePolicy } from './usePolicy';

const SourceContainer = styled.div`
    overflow: auto;
    display: flex;
    flex-direction: column;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const PolicyName = styled.span`
    cursor: pointer;
    font-weight: 700;
`;

const PoliciesType = styled(Tag)`
    && {
        border-radius: 2px !important;
        font-weight: 700;
    }
`;

const ActorTag = styled(Tag)`
    && {
        display: inline-block;
        text-align: center;
    }
`;

const ActionButtonContainer = styled.div`
    display: flex;
    justify-content: right;
`;

const EditPolicyButton = styled(Button)`
    margin-right: 16px;
`;

const PageContainer = styled.span`
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
`;
const StyledSelect = styled(Select)`
    margin-right: 15px;
    min-width: 90px;
    margin-left: 20px;
`;

const SelectContainer = styled.div`
    display: flex;
    align-items: flex-start;
`;

export enum StatusType {
    ALL,
    ACTIVE,
    INACTIVE,
}

// TODO: Cleanup the styling.
export const ManagePolicies = () => {
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [orFilters, setOrFilters] = useState<AndFilterInput[]>([
        { and: [{ field: 'state', values: ['ACTIVE'], condition: FilterOperator.Equal }] },
    ]);
    const [statusFilter, setStatusFilter] = useState(StatusType.ACTIVE);

    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const {
        config: { policiesConfig },
    } = useAppConfig();

    // Policy list paging.
    const [page, setPage] = useState(1);
    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    // Controls whether the editing and details view modals are active.
    const [showPolicyBuilderModal, setShowPolicyBuilderModal] = useState(false);
    const [showViewPolicyModal, setShowViewPolicyModal] = useState(false);

    // Focused policy represents a policy being actively viewed, edited, created via a popup modal.
    const [focusPolicyUrn, setFocusPolicyUrn] = useState<undefined | string>(undefined);
    const [focusPolicy, setFocusPolicy] = useState<Omit<Policy, 'urn'>>(EMPTY_POLICY);

    const {
        loading: policiesLoading,
        error: policiesError,
        data: policiesData,
        refetch: policiesRefetch,
    } = useListPoliciesQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query,
                orFilters,
            },
        },
        fetchPolicy: (query?.length || 0) > 0 ? 'no-cache' : 'cache-first',
    });

    const totalPolicies = policiesData?.listPolicies?.total || 0;
    const policies = useMemo(() => policiesData?.listPolicies?.policies || [], [policiesData]);

    const onChangePage = (newPage: number) => {
        scrollToTop();
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
        setFocusPolicyUrn(policy?.urn);
        setFocusPolicy({ ...policy });
    };

    const onCancelViewPolicy = () => {
        setShowViewPolicyModal(false);
        setFocusPolicy(EMPTY_POLICY);
        setFocusPolicyUrn(undefined);
    };

    const onEditPolicy = (policy: Policy) => {
        setShowPolicyBuilderModal(true);
        setFocusPolicyUrn(policy?.urn);
        setFocusPolicy({ ...policy });
    };

    const onStatusChange = (newStatusFilter: StatusType) => {
        setStatusFilter(newStatusFilter);
        // Reset page to 1 when filter changes
        setPage(1);
        const filtersInput: any = [];
        let statusValue = '';
        if (newStatusFilter === StatusType.ACTIVE) {
            statusValue = 'ACTIVE';
        } else if (newStatusFilter === StatusType.INACTIVE) {
            statusValue = 'INACTIVE';
        }
        if (statusValue) {
            const filter = { field: 'state', values: [statusValue], condition: FilterOperator.Equal };
            filtersInput.push({ and: [filter] });
        }
        setOrFilters(filtersInput);
    };

    useEffect(() => {
        policiesRefetch();
    }, [orFilters, policiesRefetch]);

    const {
        createPolicyError,
        updatePolicyError,
        deletePolicyError,
        onSavePolicy,
        onToggleActiveDuplicate,
        onRemovePolicy,
        getPrivilegeNames,
    } = usePolicy(
        policiesConfig,
        focusPolicyUrn,
        policiesRefetch,
        setShowViewPolicyModal,
        onCancelViewPolicy,
        onClosePolicyBuilder,
    );

    const updateError = createPolicyError || updatePolicyError || deletePolicyError;

    const tableColumns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            render: (_, record: any) => {
                return (
                    <PolicyName
                        onClick={() => onViewPolicy(record.policy)}
                        style={{ color: record?.editable ? '#000000' : '#8C8C8C' }}
                    >
                        {record?.name}
                    </PolicyName>
                );
            },
        },
        {
            title: 'Type',
            dataIndex: 'type',
            key: 'type',
            render: (type: string) => {
                const policyType = type?.charAt(0)?.toUpperCase() + type?.slice(1)?.toLowerCase();
                return <PoliciesType>{policyType}</PoliciesType>;
            },
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            render: (description: string) => description || '',
        },
        {
            title: 'Actors',
            dataIndex: 'actors',
            key: 'actors',
            render: (_, record: any) => {
                return (
                    <>
                        <AvatarsGroup
                            users={record?.resolvedUsers}
                            groups={record?.resolvedGroups}
                            entityRegistry={entityRegistry}
                            maxCount={3}
                            size={28}
                        />
                        {record?.allUsers ? <ActorTag>All Users</ActorTag> : null}
                        {record?.allGroups ? <ActorTag>All Groups</ActorTag> : null}
                        {record?.resourceOwners ? <ActorTag>All Owners</ActorTag> : null}
                    </>
                );
            },
        },
        {
            title: 'State',
            dataIndex: 'state',
            key: 'state',
            render: (state: string) => {
                const isActive = state === PolicyState.Active;
                return <Tag color={isActive ? 'green' : 'red'}>{state}</Tag>;
            },
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (_, record: any) => (
                <ActionButtonContainer>
                    <EditPolicyButton disabled={!record?.editable} onClick={() => onEditPolicy(record?.policy)}>
                        EDIT
                    </EditPolicyButton>
                    {record?.state === PolicyState.Active ? (
                        <Button
                            disabled={!record?.editable}
                            onClick={() => {
                                onToggleActiveDuplicate(record?.policy);
                                analytics.event({
                                    type: EventType.DeactivatePolicyEvent,
                                    policyUrn: record?.policy?.urn,
                                });
                            }}
                            style={{ color: record?.editable ? 'red' : ANTD_GRAY[6], width: 100 }}
                        >
                            DEACTIVATE
                        </Button>
                    ) : (
                        <Button
                            disabled={!record?.editable}
                            onClick={() => {
                                onToggleActiveDuplicate(record?.policy);
                                analytics.event({
                                    type: EventType.ActivatePolicyEvent,
                                    policyUrn: record?.policy?.urn,
                                });
                            }}
                            style={{ color: record?.editable ? 'green' : ANTD_GRAY[6], width: 100 }}
                        >
                            ACTIVATE
                        </Button>
                    )}
                    <Button
                        disabled={!record?.editable}
                        onClick={() => onRemovePolicy(record?.policy)}
                        type="text"
                        shape="circle"
                        danger
                    >
                        <DeleteOutlined />
                    </Button>
                </ActionButtonContainer>
            ),
        },
    ];

    const tableData = policies?.map((policy) => ({
        allGroups: policy?.actors?.allGroups,
        allUsers: policy?.actors?.allUsers,
        resourceOwners: policy?.actors?.resourceOwners,
        description: policy?.description,
        editable: policy?.editable,
        name: policy?.name,
        privileges: policy?.privileges,
        policy,
        resolvedGroups: policy?.actors?.resolvedGroups,
        resolvedUsers: policy?.actors?.resolvedUsers,
        resources: policy?.resources,
        state: policy?.state,
        type: policy?.type,
        urn: policy?.urn,
    }));

    return (
        <PageContainer>
            <OnboardingTour stepIds={[POLICIES_INTRO_ID, POLICIES_CREATE_POLICY_ID]} />
            {policiesLoading && !policiesData && (
                <Message type="loading" content="Loading policies..." style={{ marginTop: '10%' }} />
            )}
            {policiesError && <Message type="error" content="Failed to load policies! An unexpected error occurred." />}
            {updateError && message.error('Failed to update policies. An unexpected error occurred.')}
            <SourceContainer>
                <TabToolbar>
                    <div>
                        <Button
                            id={POLICIES_CREATE_POLICY_ID}
                            type="text"
                            onClick={onClickNewPolicy}
                            data-testid="add-policy-button"
                        >
                            <PlusOutlined /> Create new policy
                        </Button>
                    </div>
                    <SelectContainer>
                        <SearchBar
                            initialQuery={query || ''}
                            placeholderText="Search policies..."
                            suggestions={[]}
                            style={{
                                maxWidth: 220,
                                padding: 0,
                            }}
                            inputStyle={{
                                height: 32,
                                fontSize: 12,
                            }}
                            onSearch={() => null}
                            onQueryChange={(q) => {
                                setPage(1);
                                setQuery(q);
                            }}
                            entityRegistry={entityRegistry}
                            hideRecommendations
                        />
                        <StyledSelect
                            value={statusFilter}
                            onChange={(selection) => onStatusChange(selection as StatusType)}
                            style={{ width: 100 }}
                        >
                            <Select.Option value={StatusType.ALL} key="ALL">
                                All
                            </Select.Option>
                            <Select.Option value={StatusType.ACTIVE} key="ACTIVE">
                                Active
                            </Select.Option>
                            <Select.Option value={StatusType.INACTIVE} key="INACTIVE">
                                Inactive
                            </Select.Option>
                        </StyledSelect>
                    </SelectContainer>
                </TabToolbar>
                <StyledTable
                    columns={tableColumns}
                    dataSource={tableData}
                    rowKey="urn"
                    locale={{
                        emptyText: <Empty description="No Policies!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    pagination={false}
                />
            </SourceContainer>
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
            {showPolicyBuilderModal && (
                <PolicyBuilderModal
                    focusPolicyUrn={focusPolicyUrn}
                    policy={focusPolicy || EMPTY_POLICY}
                    setPolicy={setFocusPolicy}
                    open={showPolicyBuilderModal}
                    onClose={onClosePolicyBuilder}
                    onSave={onSavePolicy}
                />
            )}
            {showViewPolicyModal && (
                <PolicyDetailsModal
                    policy={focusPolicy}
                    open={showViewPolicyModal}
                    onClose={onCancelViewPolicy}
                    privileges={getPrivilegeNames(focusPolicy)}
                />
            )}
        </PageContainer>
    );
};
