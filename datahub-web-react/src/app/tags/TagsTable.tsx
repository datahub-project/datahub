import { NetworkStatus } from '@apollo/client';
import { Modal, Table } from '@components';
import { message } from 'antd';
import React, { useCallback, useMemo, useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { ManageTag } from '@app/tags/ManageTag';
import {
    TagActionsColumn,
    TagAppliedToColumn,
    TagColorColumn,
    TagDescriptionColumn,
    TagNameColumn,
    TagOwnersColumn,
} from '@app/tags/TagsTableColumns';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { GetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';

import { useDeleteTagMutation } from '@graphql/tag.generated';

interface Props {
    searchQuery: string;
    searchData: GetSearchResultsForMultipleQuery | undefined;
    loading: boolean;
    networkStatus: NetworkStatus;
    refetch: () => Promise<any>;
}

const TagsTable = ({ searchQuery, searchData, loading: propLoading, networkStatus, refetch }: Props) => {
    const entityRegistry = useEntityRegistry();
    const userContext = useUserContext();
    const [deleteTagMutation] = useDeleteTagMutation();

    // Check if user has permission to manage or delete tags
    const canManageTags = Boolean(userContext?.platformPrivileges?.manageTags);

    // Optimize the tagsData with useMemo to prevent unnecessary filtering on re-renders
    const tagsData = useMemo(() => {
        return searchData?.searchAcrossEntities?.searchResults || [];
    }, [searchData]);

    const [showEdit, setShowEdit] = useState(false);
    const [editingTag, setEditingTag] = useState('');

    // Simplified state for delete confirmation modal
    const [showDeleteModal, setShowDeleteModal] = useState(false);
    const [tagUrnToDelete, setTagUrnToDelete] = useState('');
    const [tagDisplayName, setTagDisplayName] = useState('');

    const [sortedInfo, setSortedInfo] = useState<{
        columnKey?: string;
        order?: 'ascend' | 'descend';
    }>({});

    // Fix the handler type to match what Table expects
    const handleTableChange = (pagination: any, filters: any, sorter: any): void => {
        setSortedInfo(sorter);
    };

    // Filter tags based on search query and sort by name - optimized with useMemo
    const filteredTags = useMemo(() => {
        return tagsData
            .filter((result) => {
                const tag = result.entity;
                const displayName = entityRegistry.getDisplayName(EntityType.Tag, tag);
                if (!searchQuery) return true;
                return displayName.toLowerCase().includes(searchQuery.toLowerCase());
            })
            .sort((a, b) => {
                const nameA = entityRegistry.getDisplayName(EntityType.Tag, a.entity);
                const nameB = entityRegistry.getDisplayName(EntityType.Tag, b.entity);
                return nameA.localeCompare(nameB);
            });
    }, [tagsData, searchQuery, entityRegistry]);

    const isLoading = propLoading || networkStatus === NetworkStatus.refetch;

    // Simplified function to initiate tag deletion
    const showDeleteConfirmation = useCallback(
        (tagUrn: string) => {
            // Find the tag entity from tagsData
            const tagData = tagsData.find((result) => result.entity.urn === tagUrn);
            if (!tagData) {
                message.error('Failed to find tag information');
                return;
            }

            const fullDisplayName = entityRegistry.getDisplayName(EntityType.Tag, tagData.entity);

            setTagUrnToDelete(tagUrn);
            setTagDisplayName(fullDisplayName);
            setShowDeleteModal(true);
        },
        [entityRegistry, tagsData],
    );

    // Function to handle the actual tag deletion
    const handleDeleteTag = useCallback(() => {
        deleteTagMutation({
            variables: {
                urn: tagUrnToDelete,
            },
        })
            .then(() => {
                message.success(`Tag "${tagDisplayName}" has been deleted`);
                refetch(); // Refresh the tag list
            })
            .catch((e: any) => {
                message.error(`Failed to delete tag: ${e.message}`);
            });

        setShowDeleteModal(false);
    }, [deleteTagMutation, refetch, tagUrnToDelete, tagDisplayName]);

    const columns = useMemo(
        () => [
            {
                title: 'Tag',
                key: 'tag',
                render: (record) => {
                    const tag = record.entity;
                    const displayName = entityRegistry.getDisplayName(EntityType.Tag, tag);
                    return <TagNameColumn tagUrn={tag.urn} displayName={displayName} searchQuery={searchQuery} />;
                },
                sorter: (sourceA, sourceB) => {
                    const nameA = entityRegistry.getDisplayName(EntityType.Tag, sourceA.entity);
                    const nameB = entityRegistry.getDisplayName(EntityType.Tag, sourceB.entity);
                    return nameA.localeCompare(nameB);
                },
                sortOrder: sortedInfo.columnKey === 'tag' ? sortedInfo.order : null,
            },
            {
                title: 'Color',
                key: 'color',
                render: (record) => {
                    return <TagColorColumn tag={record.entity} />;
                },
            },
            {
                title: 'Description',
                key: 'description',
                render: (record) => {
                    return <TagDescriptionColumn key={`description-${record.entity.urn}`} tagUrn={record.entity.urn} />;
                },
            },
            {
                title: 'Owners',
                key: 'owners',
                render: (record) => {
                    return <TagOwnersColumn key={`owners-${record.entity.urn}`} tagUrn={record.entity.urn} />;
                },
            },
            {
                title: 'Applied to',
                key: 'appliedTo',
                render: (record) => {
                    return <TagAppliedToColumn key={`applied-${record.entity.urn}`} tagUrn={record.entity.urn} />;
                },
            },
            {
                title: '',
                key: 'actions',
                alignment: 'right' as AlignmentOptions,
                render: (record) => {
                    return (
                        <TagActionsColumn
                            tagUrn={record.entity.urn}
                            onEdit={() => {
                                setEditingTag(record.entity.urn);
                                setShowEdit(true);
                            }}
                            onDelete={() => {
                                if (canManageTags) {
                                    showDeleteConfirmation(record.entity.urn);
                                } else {
                                    message.error('You do not have permission to delete tags');
                                }
                            }}
                            canManageTags={canManageTags}
                        />
                    );
                },
            },
        ],
        [entityRegistry, searchQuery, sortedInfo, canManageTags, showDeleteConfirmation],
    );

    // Generate table data once with memoization
    const tableData = useMemo(() => {
        return filteredTags.map((tag) => ({
            ...tag,
            key: tag.entity.urn,
        }));
    }, [filteredTags]);

    return (
        <>
            <Table
                columns={columns}
                data={tableData}
                isLoading={isLoading}
                isScrollable
                rowKey="key"
                onChange={handleTableChange as any}
            />

            {showEdit && (
                <ManageTag
                    tagUrn={editingTag}
                    onClose={() => setShowEdit(false)}
                    onSave={refetch}
                    isModalOpen={showEdit}
                />
            )}

            {/* Delete confirmation modal - simplified */}
            <Modal
                title={`Delete tag ${tagDisplayName}`}
                onCancel={() => setShowDeleteModal(false)}
                open={showDeleteModal}
                centered
                buttons={[
                    {
                        text: 'Cancel',
                        color: 'violet',
                        variant: 'text',
                        onClick: () => setShowDeleteModal(false),
                    },
                    {
                        text: 'Delete',
                        color: 'red',
                        variant: 'filled',
                        onClick: handleDeleteTag,
                        buttonDataTestId: 'delete-tag-button',
                    },
                ]}
            >
                <p>Are you sure you want to delete this tag? This action cannot be undone.</p>
            </Modal>
        </>
    );
};

export default TagsTable;
