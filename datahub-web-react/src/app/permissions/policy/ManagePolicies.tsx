import {
    Avatar,
    Button,
    Menu,
    Pagination,
    Pill,
    SearchBar,
    SimpleSelect,
    Table,
    Text,
    Tooltip,
    toast,
} from '@components';
import * as QueryString from 'query-string';
import React, { useEffect, useMemo, useState } from 'react';
import { useLocation } from 'react-router';
import styled, { useTheme } from 'styled-components';

import AvatarStackWithHover from '@components/components/AvatarStack/AvatarStackWithHover';
import { AvatarItemProps, AvatarType } from '@components/components/AvatarStack/types';
import { ItemType } from '@components/components/Menu/types';

import analytics, { EventType } from '@app/analytics';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { POLICIES_CREATE_POLICY_ID, POLICIES_INTRO_ID } from '@app/onboarding/config/PoliciesOnboardingConfig';
import PolicyBuilderModal from '@app/permissions/policy/PolicyBuilderModal';
import PolicyDetailsModal from '@app/permissions/policy/PolicyDetailsModal';
import { DEFAULT_PAGE_SIZE, EMPTY_POLICY } from '@app/permissions/policy/policyUtils';
import { usePolicy } from '@app/permissions/policy/usePolicy';
import { scrollToTop } from '@app/shared/searchUtils';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useListPoliciesQuery } from '@graphql/policy.generated';
import { AndFilterInput, EntityType, FilterOperator, Policy, PolicyState } from '@types';

const PageContainer = styled.div`
    width: 100%;
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding-top: 16px;
    overflow: hidden;
`;

const ToolbarContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const TableScrollContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    overflow: auto;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const PolicyName = styled.span`
    cursor: pointer;
    font-weight: 700;
`;

const ActorsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    flex-wrap: wrap;
`;

const MenuTriggerContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

const EmptyContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 40px;
`;

export enum StatusType {
    ALL = 'ALL',
    ACTIVE = 'ACTIVE',
    INACTIVE = 'INACTIVE',
}

const STATUS_OPTIONS = [
    { value: StatusType.ALL, label: 'All' },
    { value: StatusType.ACTIVE, label: 'Active' },
    { value: StatusType.INACTIVE, label: 'Inactive' },
];

interface ManagePoliciesProps {
    createPolicyRequested?: boolean;
    onCreatePolicyHandled?: () => void;
}

export const ManagePolicies = ({ createPolicyRequested, onCreatePolicyHandled }: ManagePoliciesProps) => {
    const entityRegistry = useEntityRegistry();
    const theme = useTheme();
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

    const [page, setPage] = useState(1);
    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const [showPolicyBuilderModal, setShowPolicyBuilderModal] = useState(false);
    const [showViewPolicyModal, setShowViewPolicyModal] = useState(false);

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

    useEffect(() => {
        if (createPolicyRequested) {
            onClickNewPolicy();
            onCreatePolicyHandled?.();
        }
    }, [createPolicyRequested]); // eslint-disable-line react-hooks/exhaustive-deps

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

    const onStatusChange = (selectedValues: string[]) => {
        const newStatus = (selectedValues[0] as StatusType) || StatusType.ALL;
        setStatusFilter(newStatus);
        setPage(1);
        const filtersInput: AndFilterInput[] = [];
        if (newStatus === StatusType.ACTIVE || newStatus === StatusType.INACTIVE) {
            filtersInput.push({
                and: [{ field: 'state', values: [newStatus], condition: FilterOperator.Equal }],
            });
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
            key: 'name',
            width: '20%',
            render: (record: any) => (
                <PolicyName
                    onClick={() => onViewPolicy(record.policy)}
                    style={{ color: record?.editable ? theme.colors.text : theme.colors.textSecondary }}
                >
                    {record?.name}
                </PolicyName>
            ),
        },
        {
            title: 'Type',
            key: 'type',
            width: '10%',
            render: (record: any) => {
                const policyType = record?.type?.charAt(0)?.toUpperCase() + record?.type?.slice(1)?.toLowerCase();
                const isPlatform = record?.type?.toUpperCase() === 'PLATFORM';
                return (
                    <Pill
                        label={policyType}
                        color={isPlatform ? 'blue' : 'violet'}
                        variant="filled"
                        size="sm"
                        clickable={false}
                    />
                );
            },
        },
        {
            title: 'Description',
            key: 'description',
            width: '25%',
            render: (record: any) => record?.description || '',
        },
        {
            title: 'Actors',
            key: 'actors',
            width: '20%',
            render: (record: any) => {
                const avatars: AvatarItemProps[] = [
                    ...(record?.resolvedUsers || []).map((user) => ({
                        name: entityRegistry.getDisplayName(EntityType.CorpUser, user),
                        imageUrl: user?.editableProperties?.pictureLink || undefined,
                        urn: user?.urn,
                        type: AvatarType.user,
                    })),
                    ...(record?.resolvedGroups || []).map((group) => ({
                        name: entityRegistry.getDisplayName(EntityType.CorpGroup, group),
                        urn: group?.urn,
                        type: AvatarType.group,
                    })),
                ];

                return (
                    <ActorsContainer>
                        {avatars.length === 1 && (
                            <Tooltip title={avatars[0].name}>
                                <Avatar
                                    name={avatars[0].name}
                                    imageUrl={avatars[0].imageUrl}
                                    type={avatars[0].type}
                                    size="sm"
                                    showInPill
                                />
                            </Tooltip>
                        )}
                        {avatars.length > 1 && (
                            <AvatarStackWithHover
                                avatars={avatars}
                                maxToShow={3}
                                size="sm"
                                showRemainingNumber
                                totalCount={avatars.length}
                                entityRegistry={entityRegistry as any}
                                title="Actors"
                            />
                        )}
                        {record?.allUsers && (
                            <Pill label="All Users" variant="outline" color="gray" size="sm" clickable={false} />
                        )}
                        {record?.allGroups && (
                            <Pill label="All Groups" variant="outline" color="gray" size="sm" clickable={false} />
                        )}
                        {record?.resourceOwners && (
                            <Pill label="All Owners" variant="outline" color="gray" size="sm" clickable={false} />
                        )}
                    </ActorsContainer>
                );
            },
        },
        {
            title: 'State',
            key: 'state',
            width: '10%',
            render: (record: any) => {
                const isActive = record?.state === PolicyState.Active;
                return (
                    <Pill
                        label={record?.state?.charAt(0) + record?.state?.slice(1)?.toLowerCase()}
                        color={isActive ? 'green' : 'red'}
                        size="sm"
                        clickable={false}
                    />
                );
            },
        },
        {
            title: '',
            key: 'actions',
            width: '60px',
            alignment: 'right' as const,
            render: (record: any) => {
                const isActive = record?.state === PolicyState.Active;
                if (!record?.editable) {
                    return (
                        <MenuTriggerContainer onClick={(e) => e.stopPropagation()}>
                            <Tooltip title="This is a system policy and cannot be modified">
                                <span style={{ display: 'inline-flex', cursor: 'not-allowed' }}>
                                    <Button
                                        variant="text"
                                        isCircle
                                        disabled
                                        style={{ pointerEvents: 'none', opacity: 0.4 }}
                                        icon={{
                                            icon: 'DotsThreeVertical',
                                            source: 'phosphor',
                                            weight: 'bold',
                                            size: 'xl',
                                            color: 'gray',
                                        }}
                                    />
                                </span>
                            </Tooltip>
                        </MenuTriggerContainer>
                    );
                }

                const menuItems: ItemType[] = [
                    {
                        type: 'item',
                        key: 'edit',
                        title: 'Edit',
                        icon: 'PencilSimple',
                        onClick: () => onEditPolicy(record?.policy),
                    },
                    {
                        type: 'item',
                        key: 'toggle-active',
                        title: isActive ? 'Deactivate' : 'Activate',
                        icon: isActive ? 'Pause' : 'Play',
                        onClick: () => {
                            onToggleActiveDuplicate(record?.policy);
                            analytics.event({
                                type: isActive ? EventType.DeactivatePolicyEvent : EventType.ActivatePolicyEvent,
                                policyUrn: record?.policy?.urn,
                            });
                        },
                    },
                    { type: 'divider', key: 'divider' },
                    {
                        type: 'item',
                        key: 'delete',
                        title: 'Delete',
                        icon: 'Trash',
                        danger: true,
                        onClick: () => onRemovePolicy(record?.policy),
                    },
                ];

                return (
                    <MenuTriggerContainer onClick={(e) => e.stopPropagation()}>
                        <Menu items={menuItems} placement="bottomRight">
                            <Button
                                variant="text"
                                isCircle
                                icon={{
                                    icon: 'DotsThreeVertical',
                                    source: 'phosphor',
                                    weight: 'bold',
                                    size: 'xl',
                                    color: 'gray',
                                }}
                            />
                        </Menu>
                    </MenuTriggerContainer>
                );
            },
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
            {policiesError && toast.error('Failed to load policies! An unexpected error occurred.')}
            {updateError && toast.error('Failed to update policies. An unexpected error occurred.')}
            <ToolbarContainer>
                <SearchBar
                    placeholder="Search policies..."
                    value={query || ''}
                    onChange={(value) => {
                        setPage(1);
                        setQuery(value);
                    }}
                    width="250px"
                    allowClear
                />
                <SimpleSelect
                    options={STATUS_OPTIONS}
                    values={[statusFilter]}
                    onUpdate={onStatusChange}
                    showClear={false}
                    width={120}
                    minWidth="120px"
                    position="start"
                    size="md"
                    dataTestId="policy-filter"
                />
            </ToolbarContainer>
            <TableScrollContainer>
                {!policiesLoading && tableData?.length === 0 ? (
                    <EmptyContainer>
                        <Text size="md" color="gray">
                            No Policies!
                        </Text>
                    </EmptyContainer>
                ) : (
                    <Table
                        columns={tableColumns}
                        data={tableData || []}
                        rowKey="urn"
                        isScrollable
                        isLoading={policiesLoading}
                        style={{ tableLayout: 'fixed' }}
                        onRowClick={(record: any) => onViewPolicy(record.policy)}
                    />
                )}
            </TableScrollContainer>
            <PaginationContainer>
                <Pagination
                    currentPage={page}
                    itemsPerPage={pageSize}
                    total={totalPolicies}
                    onPageChange={onChangePage}
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
