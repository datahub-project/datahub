import { Button, Menu, Pill, Table, Text, Tooltip } from '@components';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import React, { useRef, useState } from 'react';
import Highlight from 'react-highlighter';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import { useTheme } from 'styled-components';

import { TableWithInfiniteScroll } from '@components/components/Table/TableWithInfiniteScroll';

import EmptyStructuredProperties from '@app/govern/structuredProperties/EmptyStructuredProperties';
import PlatformAvatarStack from '@app/govern/structuredProperties/PlatformAvatarStack';
import {
    ActionsContainer,
    DataContainer,
    IconContainer,
    NameColumn,
    PillContainer,
    PillsContainer,
    PropDescription,
    PropName,
} from '@app/govern/structuredProperties/styledComponents';
import {
    getDisplayName,
    getFilteredSortedStructuredProperties,
    getValueTypeLabel,
} from '@app/govern/structuredProperties/utils';
import ActorPill from '@app/sharedV2/owners/ActorPill';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import analytics, { EventType } from '@src/app/analytics';
import { useUserContext } from '@src/app/context/useUserContext';
import { toLocalDateString, toRelativeTimeString } from '@src/app/shared/time/timeUtils';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { ToastType, showToastMessage } from '@src/app/sharedV2/toastMessageUtils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { PageRoutes } from '@src/conf/Global';
import { useBatchUpdateSoftDeletedMutation } from '@src/graphql/mutations.generated';
import { useDeleteStructuredPropertyMutation } from '@src/graphql/structuredProperties.generated';
import TableIcon from '@src/images/table-icon.svg?react';
import { DataPlatform, Entity, EntityType, PropertyCardinality, StructuredPropertyEntity } from '@src/types.generated';

const LIST_SEPARATOR = ', ';
const MAX_PILLS_TO_SHOW = 2;

const PillList = ({ labels }: { labels: string[] }) => {
    const overflowCount = labels.length - MAX_PILLS_TO_SHOW;

    return (
        <PillsContainer>
            {labels.slice(0, MAX_PILLS_TO_SHOW).map((label) => (
                <PillContainer key={label}>
                    <Pill label={label} clickable={false} />
                </PillContainer>
            ))}
            {overflowCount > 0 && (
                <Tooltip title={labels.slice(MAX_PILLS_TO_SHOW).join(LIST_SEPARATOR)} showArrow={false}>
                    <>
                        <Text>{`+${overflowCount}`}</Text>
                    </>
                </Tooltip>
            )}
        </PillsContainer>
    );
};

const getPropertyTypeLabel = (property: StructuredPropertyEntity) =>
    getValueTypeLabel(property.definition.valueType.urn, property.definition.cardinality || PropertyCardinality.Single);

type Props = {
    searchQuery: string;
    loading: boolean;
    fetchData: (start: number, count: number) => Promise<Entity[]>;
    totalCount?: number;
    setTotalCount?: React.Dispatch<React.SetStateAction<number>>;
    pageSize: number;
    searchResults?: Entity[] | null;
    isSearchLoading?: boolean;
};

const StructuredPropsTable = ({
    searchQuery,
    loading,
    fetchData,
    totalCount,
    setTotalCount,
    pageSize,
    searchResults,
    isSearchLoading,
}: Props) => {
    const { t } = useTranslation('governance.structured-properties');
    const { t: tc } = useTranslation('common.actions');
    const { t: tl } = useTranslation('common.labels');
    const theme = useTheme();
    const entityRegistry = useEntityRegistry();
    const me = useUserContext();
    const canEditProps = me.platformPrivileges?.manageStructuredProperties;
    const history = useHistory();

    const openPropertyPage = (property: StructuredPropertyEntity, readOnly = !canEditProps) => {
        history.push(PageRoutes.STRUCTURED_PROPERTIES_EDIT.replace(':urn', encodeURIComponent(property.urn)), {
            readOnly,
        });
        analytics.event({
            type: EventType.ViewStructuredPropertyEvent,
            propertyUrn: property.urn,
        });
    };

    const structuredProperties = (searchQuery && (searchResults as StructuredPropertyEntity[])) || [];

    // Filter on the displayed name (displayName, falling back to qualifiedName) and sort by newest first.
    const filteredProperties = getFilteredSortedStructuredProperties(structuredProperties, searchQuery);

    const [deleteStructuredProperty] = useDeleteStructuredPropertyMutation();
    const [batchUpdateSoftDeleted] = useBatchUpdateSoftDeletedMutation();

    const [showConfirmDelete, setShowConfirmDelete] = useState(false);
    const [propertyToDelete, setPropertyToDelete] = useState<StructuredPropertyEntity>();
    const [deletedPropertyUrns, setDeletedPropertyUrns] = useState<string[]>([]);
    const deleteInProgress = useRef(false);

    const handleDeleteProperty = async () => {
        if (!propertyToDelete || deleteInProgress.current) return;

        deleteInProgress.current = true;
        showToastMessage(ToastType.LOADING, t('table.deleting'), 1);
        try {
            // Soft-delete first: the backend rejects hard deletion of an active structured property,
            // since hard deletion can permanently reserve the property's qualified name in the search
            // index. Only hard-delete once the soft delete has succeeded.
            await batchUpdateSoftDeleted({
                variables: {
                    input: {
                        urns: [propertyToDelete.urn],
                        deleted: true,
                    },
                },
            });
            await deleteStructuredProperty({
                variables: {
                    input: {
                        urn: propertyToDelete.urn,
                    },
                },
            });
            analytics.event({
                type: EventType.DeleteStructuredPropertyEvent,
                propertyUrn: propertyToDelete.urn,
                propertyType: propertyToDelete.definition.valueType.urn,
                appliesTo: propertyToDelete.definition.entityTypes.map((type) => type.urn),
                qualifiedName: propertyToDelete.definition.qualifiedName,
                showInFilters: propertyToDelete.settings?.showInSearchFilters,
                allowedAssetTypes: propertyToDelete.definition.typeQualifier?.allowedTypes?.map(
                    (allowedType) => allowedType.urn,
                ),
                allowedValues: propertyToDelete.definition.allowedValues || undefined,
                cardinality: propertyToDelete.definition.cardinality || undefined,
                isHidden: propertyToDelete.settings?.isHidden ?? false,
                showInSearchFilters: propertyToDelete.settings?.showInSearchFilters ?? false,
                showAsAssetBadge: propertyToDelete.settings?.showAsAssetBadge ?? false,
                showInAssetSummary: propertyToDelete.settings?.showInAssetSummary ?? false,
                hideInAssetSummaryWhenEmpty: propertyToDelete.settings?.hideInAssetSummaryWhenEmpty ?? false,
                showInColumnsTable: propertyToDelete.settings?.showInColumnsTable ?? false,
            });
            showToastMessage(ToastType.SUCCESS, t('table.deleteSuccess'), 3);
            setDeletedPropertyUrns((urns) => [...urns, propertyToDelete.urn]);
            setTotalCount?.((prev) => Math.max(0, prev - 1));
        } catch {
            showToastMessage(ToastType.ERROR, t('table.deleteError'), 3);
        } finally {
            deleteInProgress.current = false;
            setShowConfirmDelete(false);
            setPropertyToDelete(undefined);
        }
    };

    const handleDeleteClose = () => {
        if (deleteInProgress.current) return;
        setShowConfirmDelete(false);
        setPropertyToDelete(undefined);
    };

    if (!loading && !isSearchLoading && !filteredProperties.length && searchQuery) {
        return <EmptyStructuredProperties isEmptySearch />;
    }

    const columns = [
        {
            title: tl('name'),
            key: 'name',
            render: (record) => {
                return (
                    <NameColumn>
                        <IconContainer>
                            <TableIcon color={theme.colors.iconBrand} />
                        </IconContainer>
                        <DataContainer>
                            <PropName title={getDisplayName(record)}>
                                <Highlight search={searchQuery}>{getDisplayName(record)}</Highlight>
                            </PropName>
                            <PropDescription>{record.definition.description}</PropDescription>
                        </DataContainer>
                    </NameColumn>
                );
            },
            width: '470px',
            sorter: (sourceA, sourceB) => {
                return getDisplayName(sourceA).localeCompare(getDisplayName(sourceB));
            },
        },
        {
            title: t('table.typeColumn'),
            key: 'type',
            width: '150px',
            render: (record) => <Text>{getPropertyTypeLabel(record) ?? '-'}</Text>,
            sorter: (sourceA, sourceB) => {
                return (getPropertyTypeLabel(sourceA) ?? '').localeCompare(getPropertyTypeLabel(sourceB) ?? '');
            },
        },
        {
            title: t('table.entityTypesColumn'),
            key: 'entityTypes',
            width: '230px',
            render: (record) => (
                <PillList
                    labels={record.definition.entityTypes
                        .map((entityType) => entityRegistry.getEntityName(entityType.info.type))
                        .filter((typeName): typeName is string => !!typeName)}
                />
            ),
        },
        {
            title: t('table.allowedPlatformsColumn'),
            key: 'allowedPlatforms',
            // The stacked avatars overlap, so this needs far less room than a row of pills.
            width: '120px',
            render: (record) => {
                const platforms: DataPlatform[] = record.definition.allowedPlatforms ?? [];
                // No restriction means the property is available on assets from every platform.
                if (!platforms.length) {
                    return <Text color="gray">{t('allowedPlatforms.anyPlaceholder')}</Text>;
                }

                return <PlatformAvatarStack platforms={platforms} />;
            },
        },
        {
            title: t('table.creationDateColumn'),
            key: 'creationDate',
            render: (record) => {
                const createdTime = record.definition.created?.time;
                return (
                    <Tooltip title={toLocalDateString(createdTime)} showArrow={false}>
                        {createdTime ? toRelativeTimeString(createdTime) : '-'}
                    </Tooltip>
                );
            },
            sorter: (sourceA, sourceB) => {
                const timeA = sourceA.definition.created?.time || Number.MAX_SAFE_INTEGER;
                const timeB = sourceB.definition.created?.time || Number.MAX_SAFE_INTEGER;

                return timeA - timeB;
            },
        },

        {
            title: t('table.createdByColumn'),
            key: 'createdBy',
            render: (record) => {
                const createdByUser = record.definition?.created?.actor;

                return <>{createdByUser && <ActorPill actor={createdByUser} />}</>;
            },
            sorter: (sourceA, sourceB) => {
                const createdByUserA = sourceA.definition?.created?.actor;
                const nameA = createdByUserA && entityRegistry.getDisplayName(EntityType.CorpUser, createdByUserA);
                const createdByUserB = sourceB.definition?.created?.actor;
                const nameB = createdByUserB && entityRegistry.getDisplayName(EntityType.CorpUser, createdByUserB);

                return nameA?.localeCompare(nameB);
            },
        },
        {
            title: '',
            key: 'actions',
            alignment: 'right' as AlignmentOptions,
            render: (record) => {
                const items = [
                    {
                        type: 'item' as const,
                        key: '0',
                        title: tc('view'),
                        dataTestId: 'structured-prop-action-view',
                        onClick: () => {
                            openPropertyPage(record, true);
                        },
                    },
                    {
                        type: 'item' as const,
                        key: '1',
                        title: t('table.copyUrn'),
                        dataTestId: 'structured-prop-action-copy-urn',
                        onClick: () => {
                            navigator.clipboard.writeText(record.urn);
                        },
                    },
                    {
                        type: 'item' as const,
                        key: '2',
                        title: tc('edit'),
                        dataTestId: 'structured-prop-action-edit',
                        disabled: !canEditProps,
                        tooltip: !canEditProps ? t('permissionTooltip') : undefined,
                        onClick: () => {
                            if (canEditProps) {
                                openPropertyPage(record, false);
                            }
                        },
                    },
                    {
                        type: 'item' as const,
                        key: '3',
                        title: tc('delete'),
                        dataTestId: 'structured-prop-action-delete',
                        disabled: !canEditProps,
                        danger: true,
                        tooltip: !canEditProps ? t('permissionTooltip') : undefined,
                        onClick: () => {
                            if (canEditProps) {
                                setPropertyToDelete(record);
                                setShowConfirmDelete(true);
                            }
                        },
                    },
                ];
                return (
                    <ActionsContainer onClick={(event) => event.stopPropagation()}>
                        <Menu items={items} trigger={['click']}>
                            <Button
                                variant="text"
                                isCircle
                                icon={{ icon: DotsThreeVertical, weight: 'bold', size: 'xl', color: 'gray' }}
                                data-testid="structured-props-more-options-icon"
                            />
                        </Menu>
                    </ActionsContainer>
                );
            },
        },
    ];
    return (
        <>
            {searchQuery ? (
                <Table
                    columns={columns}
                    data={filteredProperties}
                    isLoading={loading}
                    isScrollable
                    data-testid="structured-props-table"
                    rowDataTestId={(row) => row.urn}
                    onRowClick={openPropertyPage}
                />
            ) : (
                <TableWithInfiniteScroll
                    columns={columns}
                    fetchData={fetchData}
                    pageSize={pageSize}
                    totalItemCount={totalCount ?? 0}
                    data-testid="structured-props-table"
                    itemToRemove={
                        deletedPropertyUrns.length ? (item) => deletedPropertyUrns.includes(item.urn) : undefined
                    }
                    resetTrigger={searchQuery}
                    onRowClick={(record) => openPropertyPage(record as StructuredPropertyEntity)}
                    emptyState={<EmptyStructuredProperties />}
                />
            )}
            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={handleDeleteClose}
                handleConfirm={handleDeleteProperty}
                modalTitle={t('table.confirmDeleteTitle')}
                modalText={t('table.confirmDeleteText')}
            />
        </>
    );
};

export default StructuredPropsTable;
