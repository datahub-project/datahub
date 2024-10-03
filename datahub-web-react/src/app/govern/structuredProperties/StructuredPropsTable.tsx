import { useApolloClient } from '@apollo/client';
import { Icon, Pill, Table, Text } from '@components';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { HoverEntityTooltip } from '@src/app/recommendations/renderer/component/HoverEntityTooltip';
import { CustomAvatar } from '@src/app/shared/avatar';
import { toRelativeTimeString } from '@src/app/shared/time/timeUtils';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { showToastMessage, ToastType } from '@src/app/sharedV2/toastMessageUtils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { GetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { useDeleteStructuredPropertyMutation } from '@src/graphql/structuredProperties.generated';
import TableIcon from '@src/images/table-icon.svg?react';
import { Entity, EntityType, SearchResult, StructuredPropertyEntity } from '@src/types.generated';
import { Dropdown, Tooltip } from 'antd';
import React, { useState } from 'react';
import { CardIcons } from '../Dashboard/Forms/styledComponents';
import { removeFromPropertiesList } from './cacheUtils';
import {
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
    selectedProperty?: SearchResult;
    setSelectedProperty: React.Dispatch<React.SetStateAction<SearchResult | undefined>>;
    inputs: object;
    searchAcrossEntities?: object | null;
}

const StructuredPropsTable = ({
    searchQuery,
    data,
    loading,
    setIsDrawerOpen,
    selectedProperty,
    setSelectedProperty,
    inputs,
    searchAcrossEntities,
}: Props) => {
    const entityRegistry = useEntityRegistryV2();
    const client = useApolloClient();

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
        showToastMessage(ToastType.LOADING, 'Deleting structured property', 1);
        deleteStructuredProperty({
            variables: {
                input: {
                    urn: property.entity.urn,
                },
            },
        })
            .then(() => {
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
                            <div>
                                <PropName
                                    ellipsis={{ tooltip: getDisplayName(record.entity) }}
                                    onClick={() => {
                                        setIsDrawerOpen(true);
                                        setSelectedProperty(record);
                                    }}
                                >
                                    {getDisplayName(record.entity)}
                                </PropName>
                            </div>
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
                return createdTime ? toRelativeTimeString(createdTime) : '-';
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
                            <HoverEntityTooltip entity={createdByUser as Entity} showArrow={false}>
                                <CreatedByContainer>
                                    <CustomAvatar size={20} name={name} photoUrl={avatarUrl} />
                                    <Text size="sm">{name}</Text>
                                </CreatedByContainer>
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
                                    setIsDrawerOpen(true);
                                    setSelectedProperty(record);
                                }}
                            >
                                Edit
                            </MenuItem>
                        ),
                    },
                    {
                        key: '1',
                        label: (
                            <MenuItem
                                onClick={() => {
                                    setSelectedProperty(record);
                                    setShowConfirmDelete(true);
                                }}
                            >
                                Delete
                            </MenuItem>
                        ),
                    },
                ];
                return (
                    <>
                        <CardIcons>
                            <Dropdown menu={{ items }} trigger={['click']}>
                                <Icon icon="MoreVert" size="md" />
                            </Dropdown>
                        </CardIcons>
                    </>
                );
            },
        },
    ];
    return (
        <>
            <Table columns={columns} data={filteredProperties} isLoading={loading} isScrollable />
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
