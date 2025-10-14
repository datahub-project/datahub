import { Icon, Menu, Pill, Table, Text, Tooltip } from '@components';
import React, { useState } from 'react';
import Highlight from 'react-highlighter';

import { TableWithInfiniteScroll } from '@components/components/Table/TableWithInfiniteScroll';

import EmptyStructuredProperties from '@app/govern/structuredProperties/EmptyStructuredProperties';
import {
    CardIcons,
    DataContainer,
    IconContainer,
    NameColumn,
    PillContainer,
    PillsContainer,
    PropDescription,
    PropName,
} from '@app/govern/structuredProperties/styledComponents';
import { getDisplayName } from '@app/govern/structuredProperties/utils';
import ActorPill from '@app/sharedV2/owners/ActorPill';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import analytics, { EventType } from '@src/app/analytics';
import { useUserContext } from '@src/app/context/useUserContext';
import { toLocalDateString, toRelativeTimeString } from '@src/app/shared/time/timeUtils';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { ToastType, showToastMessage } from '@src/app/sharedV2/toastMessageUtils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { GetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { useDeleteStructuredPropertyMutation } from '@src/graphql/structuredProperties.generated';
import TableIcon from '@src/images/table-icon.svg?react';
import { Entity, EntityType, StructuredPropertyEntity } from '@src/types.generated';

interface Props {
    searchQuery: string;
    data: GetSearchResultsForMultipleQuery | undefined;
    loading: boolean;
    setIsDrawerOpen: React.Dispatch<React.SetStateAction<boolean>>;
    setIsViewDrawerOpen: React.Dispatch<React.SetStateAction<boolean>>;
    selectedProperty?: StructuredPropertyEntity;
    setSelectedProperty: React.Dispatch<React.SetStateAction<StructuredPropertyEntity | undefined>>;
    fetchData: (start: number, count: number) => Promise<Entity[]>;
    totalCount?: number;
    setTotalCount?: React.Dispatch<React.SetStateAction<number>>;
    pageSize: number;
    searchResults?: Entity[] | null;
    newProperty?: StructuredPropertyEntity;
    updatedProperty?: StructuredPropertyEntity;
}

const StructuredPropsTable = ({
    searchQuery,
    data,
    loading,
    setIsDrawerOpen,
    setIsViewDrawerOpen,
    selectedProperty,
    setSelectedProperty,
    fetchData,
    totalCount,
    setTotalCount,
    pageSize,
    searchResults,
    newProperty,
    updatedProperty,
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const me = useUserContext();
    const canEditProps = me.platformPrivileges?.manageStructuredProperties;

    const structuredProperties = searchQuery
        ? searchResults || []
        : data?.searchAcrossEntities?.searchResults.map((res) => res.entity) || [];

    const [deleteStructuredProperty] = useDeleteStructuredPropertyMutation();

    const [showConfirmDelete, setShowConfirmDelete] = useState<boolean>(false);

    const [propertyToDelete, setPropertyToDelete] = useState<string>('');

    const handleDeleteProperty = (property) => {
        const deleteEntity = property as StructuredPropertyEntity;
        showToastMessage(ToastType.LOADING, 'Deleting structured property', 1);
        deleteStructuredProperty({
            variables: {
                input: {
                    urn: deleteEntity.urn,
                },
            },
        })
            .then(() => {
                analytics.event({
                    type: EventType.DeleteStructuredPropertyEvent,
                    propertyUrn: property.urn,
                    propertyType: deleteEntity.definition.valueType.urn,
                    appliesTo: deleteEntity.definition.entityTypes.map((type) => type.urn),
                    qualifiedName: deleteEntity.definition.qualifiedName,
                    showInFilters: deleteEntity.settings?.showInSearchFilters,
                    allowedAssetTypes: deleteEntity.definition.typeQualifier?.allowedTypes?.map(
                        (allowedType) => allowedType.urn,
                    ),
                    allowedValues: deleteEntity.definition.allowedValues || undefined,
                    cardinality: deleteEntity.definition.cardinality || undefined,
                    isHidden: deleteEntity.settings?.isHidden ?? false,
                    showInSearchFilters: deleteEntity.settings?.showInSearchFilters ?? false,
                    showAsAssetBadge: deleteEntity.settings?.showAsAssetBadge ?? false,
                    showInAssetSummary: deleteEntity.settings?.showInAssetSummary ?? false,
                    hideInAssetSummaryWhenEmpty: deleteEntity.settings?.hideInAssetSummaryWhenEmpty ?? false,
                    showInColumnsTable: deleteEntity.settings?.showInColumnsTable ?? false,
                });
                showToastMessage(ToastType.SUCCESS, 'Structured property deleted successfully!', 3);
                setPropertyToDelete(property.urn);
                setTotalCount?.((prev) => Math.max(0, prev - 1));
            })
            .catch(() => {
                showToastMessage(ToastType.ERROR, 'Failed to delete structured property', 3);
            });

        setShowConfirmDelete(false);
        setSelectedProperty(undefined);
    };

    const handleDeleteClose = () => {
        setShowConfirmDelete(false);
        setSelectedProperty(undefined);
    };

    if (!loading && !structuredProperties.length && searchQuery) {
        return <EmptyStructuredProperties isEmptySearch />;
    }

    const columns = [
        {
            title: 'Name',
            key: 'name',
            render: (record) => {
                return (
                    <NameColumn>
                        <IconContainer>
                            <TableIcon color="#705EE4" />
                        </IconContainer>
                        <DataContainer>
                            <PropName
                                ellipsis={{ tooltip: getDisplayName(record) }}
                                onClick={() => {
                                    if (canEditProps) setIsDrawerOpen(true);
                                    else setIsViewDrawerOpen(true);

                                    setSelectedProperty(record);
                                    analytics.event({
                                        type: EventType.ViewStructuredPropertyEvent,
                                        propertyUrn: record.urn,
                                    });
                                }}
                            >
                                <Highlight search={searchQuery}>{getDisplayName(record)}</Highlight>
                            </PropName>
                            <PropDescription ellipsis>{record.definition.description}</PropDescription>
                        </DataContainer>
                    </NameColumn>
                );
            },
            width: '580px',
            sorter: (sourceA, sourceB) => {
                return getDisplayName(sourceA).localeCompare(getDisplayName(sourceB));
            },
        },
        {
            title: 'Entity Types',
            key: 'entityTypes',
            width: '270px',
            render: (record) => {
                const types = record.definition.entityTypes;
                const maxTypesToShow = 2;
                const overflowCount = types.length - maxTypesToShow;

                return (
                    <PillsContainer>
                        {types.slice(0, maxTypesToShow).map((entityType) => {
                            const typeName = entityRegistry.getEntityName(entityType.info.type);
                            return (
                                <PillContainer>{typeName && <Pill label={typeName} clickable={false} />}</PillContainer>
                            );
                        })}
                        {overflowCount > 0 && (
                            <Tooltip
                                title={types
                                    .slice(maxTypesToShow)
                                    .map((eType) => {
                                        const name = entityRegistry.getEntityName(eType.info.type);
                                        return name;
                                    })
                                    .join(', ')}
                                showArrow={false}
                            >
                                <>
                                    <Text>{`+${overflowCount}`}</Text>
                                </>
                            </Tooltip>
                        )}
                    </PillsContainer>
                );
            },
        },
        {
            title: 'Creation Date',
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
            title: 'Created By',
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
                        title: 'View',
                        onClick: () => {
                            setIsViewDrawerOpen(true);
                            setSelectedProperty(record);
                            analytics.event({
                                type: EventType.ViewStructuredPropertyEvent,
                                propertyUrn: record.urn,
                            });
                        },
                    },
                    {
                        type: 'item' as const,
                        key: '1',
                        title: 'Copy Urn',
                        onClick: () => {
                            navigator.clipboard.writeText(record.urn);
                        },
                    },
                    {
                        type: 'item' as const,
                        key: '2',
                        title: 'Edit',
                        disabled: !canEditProps,
                        tooltip: !canEditProps
                            ? 'Must have permission to manage structured properties. Ask your DataHub administrator.'
                            : undefined,
                        onClick: () => {
                            if (canEditProps) {
                                setIsDrawerOpen(true);
                                setSelectedProperty(record);
                                analytics.event({
                                    type: EventType.ViewStructuredPropertyEvent,
                                    propertyUrn: record.urn,
                                });
                            }
                        },
                    },
                    {
                        type: 'item' as const,
                        key: '3',
                        title: 'Delete',
                        disabled: !canEditProps,
                        danger: true,
                        tooltip: !canEditProps
                            ? 'Must have permission to manage structured properties. Ask your DataHub administrator.'
                            : undefined,
                        onClick: () => {
                            if (canEditProps) {
                                setSelectedProperty(record);
                                setShowConfirmDelete(true);
                            }
                        },
                    },
                ];
                return (
                    <>
                        <CardIcons>
                            <Menu items={items} trigger={['click']}>
                                <Icon icon="MoreVert" size="md" data-testid="structured-props-more-options-icon" />
                            </Menu>
                        </CardIcons>
                    </>
                );
            },
        },
    ];
    return (
        <>
            {searchQuery ? (
                <Table
                    columns={columns}
                    data={structuredProperties}
                    isLoading={loading}
                    isScrollable
                    data-testid="structured-props-table"
                />
            ) : (
                <TableWithInfiniteScroll
                    columns={columns}
                    fetchData={fetchData}
                    pageSize={pageSize}
                    totalItemCount={totalCount ?? 0}
                    data-testid="structured-props-table"
                    newItemToAdd={newProperty}
                    itemToRemove={propertyToDelete ? (item) => item.urn === propertyToDelete : undefined}
                    itemToUpdate={
                        updatedProperty
                            ? {
                                  updatedItem: updatedProperty,
                                  shouldUpdate: (item) => item.urn === updatedProperty.urn,
                              }
                            : undefined
                    }
                    resetTrigger={searchQuery}
                    emptyState={<EmptyStructuredProperties />}
                />
            )}
            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={handleDeleteClose}
                handleConfirm={() => handleDeleteProperty(selectedProperty)}
                modalTitle="Confirm Delete"
                modalText="Are you sure you want to delete? Deleting will remove this structured property from all assets it's currently on."
            />
        </>
    );
};

export default StructuredPropsTable;
