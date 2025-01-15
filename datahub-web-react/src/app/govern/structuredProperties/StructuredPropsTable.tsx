import { useApolloClient } from '@apollo/client';
import { Icon, Pill, Table, Text, Tooltip } from '@components';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import analytics, { EventType } from '@src/app/analytics';
import { useUserContext } from '@src/app/context/useUserContext';
import { HoverEntityTooltip } from '@src/app/recommendations/renderer/component/HoverEntityTooltip';
import { CustomAvatar } from '@src/app/shared/avatar';
import { toLocalDateString, toRelativeTimeString } from '@src/app/shared/time/timeUtils';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { showToastMessage, ToastType } from '@src/app/sharedV2/toastMessageUtils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { GetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { useDeleteStructuredPropertyMutation } from '@src/graphql/structuredProperties.generated';
import TableIcon from '@src/images/table-icon.svg?react';
import {
    Entity,
    EntityType,
    SearchAcrossEntitiesInput,
    SearchResult,
    SearchResults,
    StructuredPropertyEntity,
} from '@src/types.generated';
import { Dropdown } from 'antd';
import React, { useState } from 'react';
import Highlight from 'react-highlighter';
import { Link } from 'react-router-dom';
import { removeFromPropertiesList } from './cacheUtils';
import EmptyStructuredProperties from './EmptyStructuredProperties';
import {
    CardIcons,
    CreatedByContainer,
    DataContainer,
    IconContainer,
    MenuItem,
    NameColumn,
    PillContainer,
    PillsContainer,
    PropDescription,
    PropName,
} from './styledComponents';
import { getDisplayName } from './utils';

interface Props {
    searchQuery: string;
    data: GetSearchResultsForMultipleQuery | undefined;
    loading: boolean;
    setIsDrawerOpen: React.Dispatch<React.SetStateAction<boolean>>;
    setIsViewDrawerOpen: React.Dispatch<React.SetStateAction<boolean>>;
    selectedProperty?: SearchResult;
    setSelectedProperty: React.Dispatch<React.SetStateAction<SearchResult | undefined>>;
    inputs: SearchAcrossEntitiesInput;
    searchAcrossEntities?: SearchResults | null;
}

const StructuredPropsTable = ({
    searchQuery,
    data,
    loading,
    setIsDrawerOpen,
    setIsViewDrawerOpen,
    selectedProperty,
    setSelectedProperty,
    inputs,
    searchAcrossEntities,
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const client = useApolloClient();
    const me = useUserContext();
    const canEditProps = me.platformPrivileges?.manageStructuredProperties;

    const structuredProperties = data?.searchAcrossEntities?.searchResults || [];

    // Filter the table data based on the search query
    const filteredProperties = structuredProperties
        .filter((prop: any) => prop.entity.definition?.displayName?.toLowerCase().includes(searchQuery.toLowerCase()))
        .sort(
            (propA, propB) =>
                ((propB.entity as StructuredPropertyEntity).definition.created?.time || 0) -
                ((propA.entity as StructuredPropertyEntity).definition.created?.time || 0),
        );

    const [deleteStructuredProperty] = useDeleteStructuredPropertyMutation();

    const [showConfirmDelete, setShowConfirmDelete] = useState<boolean>(false);

    const handleDeleteProperty = (property) => {
        const deleteEntity = property.entity as StructuredPropertyEntity;
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
                    propertyUrn: property.entity.urn,
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
                    showInColumnsTable: deleteEntity.settings?.showInColumnsTable ?? false,
                });
                showToastMessage(ToastType.SUCCESS, 'Structured property deleted successfully!', 3);
                removeFromPropertiesList(client, inputs, property.entity.urn, searchAcrossEntities);
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

    if (!loading && !filteredProperties.length) {
        return <EmptyStructuredProperties isEmptySearch={!!structuredProperties.length} />;
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
                                ellipsis={{ tooltip: getDisplayName(record.entity) }}
                                onClick={() => {
                                    if (canEditProps) setIsDrawerOpen(true);
                                    else setIsViewDrawerOpen(true);

                                    setSelectedProperty(record);
                                    analytics.event({
                                        type: EventType.ViewStructuredPropertyEvent,
                                        propertyUrn: record.entity.urn,
                                    });
                                }}
                            >
                                <Highlight search={searchQuery}>{getDisplayName(record.entity)}</Highlight>
                            </PropName>
                            <PropDescription ellipsis>{record.entity.definition.description}</PropDescription>
                        </DataContainer>
                    </NameColumn>
                );
            },
            width: '580px',
            sorter: (sourceA, sourceB) => {
                return getDisplayName(sourceA.entity).localeCompare(getDisplayName(sourceB.entity));
            },
        },
        {
            title: 'Entity Types',
            key: 'entityTypes',
            width: '270px',
            render: (record) => {
                const types = record.entity.definition.entityTypes;
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
                const createdTime = record.entity.definition.created?.time;
                return (
                    <Tooltip title={toLocalDateString(createdTime)} showArrow={false}>
                        {createdTime ? toRelativeTimeString(createdTime) : '-'}
                    </Tooltip>
                );
            },
            sorter: (sourceA, sourceB) => {
                const timeA = sourceA.entity.definition.created?.time || Number.MAX_SAFE_INTEGER;
                const timeB = sourceB.entity.definition.created?.time || Number.MAX_SAFE_INTEGER;

                return timeA - timeB;
            },
        },

        {
            title: 'Created By',
            key: 'createdBy',
            render: (record) => {
                const createdByUser = record.entity.definition?.created?.actor;
                const name = createdByUser && entityRegistry.getDisplayName(EntityType.CorpUser, createdByUser);
                const avatarUrl = createdByUser?.editableProperties?.pictureLink || undefined;

                return (
                    <>
                        {createdByUser && (
                            <HoverEntityTooltip entity={createdByUser as Entity}>
                                <Link
                                    to={`${entityRegistry.getEntityUrl(
                                        EntityType.CorpUser,
                                        (createdByUser as Entity).urn,
                                    )}`}
                                >
                                    <CreatedByContainer>
                                        <CustomAvatar size={20} name={name} photoUrl={avatarUrl} hideTooltip />
                                        <Text color="gray" size="sm">
                                            {name}
                                        </Text>
                                    </CreatedByContainer>
                                </Link>
                            </HoverEntityTooltip>
                        )}
                    </>
                );
            },
            sorter: (sourceA, sourceB) => {
                const createdByUserA = sourceA.entity.definition?.created?.actor;
                const nameA = createdByUserA && entityRegistry.getDisplayName(EntityType.CorpUser, createdByUserA);
                const createdByUserB = sourceB.entity.definition?.created?.actor;
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
                        key: '0',
                        label: (
                            <MenuItem
                                onClick={() => {
                                    setIsViewDrawerOpen(true);
                                    setSelectedProperty(record);
                                    analytics.event({
                                        type: EventType.ViewStructuredPropertyEvent,
                                        propertyUrn: record.entity.urn,
                                    });
                                }}
                            >
                                View
                            </MenuItem>
                        ),
                    },
                    {
                        key: '1',
                        disabled: !canEditProps,
                        label: (
                            <Tooltip
                                showArrow={false}
                                title={
                                    !canEditProps
                                        ? 'Must have permission to manage structured properties. Ask your DataHub administrator.'
                                        : null
                                }
                            >
                                <MenuItem
                                    onClick={() => {
                                        if (canEditProps) {
                                            setIsDrawerOpen(true);
                                            setSelectedProperty(record);
                                            analytics.event({
                                                type: EventType.ViewStructuredPropertyEvent,
                                                propertyUrn: record.entity.urn,
                                            });
                                        }
                                    }}
                                >
                                    Edit
                                </MenuItem>
                            </Tooltip>
                        ),
                    },
                    {
                        key: '2',
                        disabled: !canEditProps,
                        label: (
                            <Tooltip
                                showArrow={false}
                                title={
                                    !canEditProps
                                        ? 'Must have permission to manage structured properties. Ask your DataHub administrator.'
                                        : null
                                }
                            >
                                <MenuItem
                                    onClick={() => {
                                        if (canEditProps) {
                                            setSelectedProperty(record);
                                            setShowConfirmDelete(true);
                                        }
                                    }}
                                >
                                    <Text color="red">Delete </Text>
                                </MenuItem>
                            </Tooltip>
                        ),
                    },
                ];
                return (
                    <>
                        <CardIcons>
                            <Dropdown menu={{ items }} trigger={['click']}>
                                <Icon icon="MoreVert" size="md" data-testid="structured-props-more-options-icon" />
                            </Dropdown>
                        </CardIcons>
                    </>
                );
            },
        },
    ];
    return (
        <>
            <Table
                columns={columns}
                data={filteredProperties}
                isLoading={loading}
                isScrollable
                data-testid="structured-props-table"
            />
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
