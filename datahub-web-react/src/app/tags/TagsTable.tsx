import React, { useState, useMemo, useCallback } from 'react';
import { NetworkStatus } from '@apollo/client';
import { Table, Modal } from '@components';
import { message } from 'antd';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { GetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';
import { useDeleteTagMutation } from '../../graphql/tag.generated';
import { useUserContext } from '../context/useUserContext';
import { ManageTag } from './ManageTag';
import {
    TagActionsColumn,
    TagAppliedToColumn,
    TagColorColumn,
    TagDescriptionColumn,
    TagNameColumn,
    TagOwnersColumn,
} from './TagsTableColumns';

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

    // State for delete confirmation modal
    const [showDeleteModal, setShowDeleteModal] = useState(false);
    const [tagToDelete, setTagToDelete] = useState<{
        urn: string;
        displayName: string;
        onConfirm: () => void;
    } | null>(null);

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

    // Function to delete a tag with confirmation
    const deleteTag = useCallback(
        (tagUrn: string) => {
            // Find the tag entity from tagsData
            const tagData = tagsData.find((result) => result.entity.urn === tagUrn);
            if (!tagData) {
                message.error('Failed to find tag information');
                return;
            }

            const fullDisplayName = entityRegistry.getDisplayName(EntityType.Tag, tagData.entity);

            // Show a confirmation modal using the @components Modal
            const confirmDeleteTag = () => {
                deleteTagMutation({
                    variables: {
                        urn: tagUrn,
                    },
                })
                    .then(() => {
                        message.success(`Tag "${fullDisplayName}" has been deleted`);
                        refetch(); // Refresh the tag list
                    })
                    .catch((e: any) => {
                        message.error(`Failed to delete tag: ${e.message}`);
                    });
            };

            // Using the modal from @components
            setShowDeleteModal(true);
            setTagToDelete({
                urn: tagUrn,
                displayName: fullDisplayName,
                onConfirm: confirmDeleteTag,
            });
        },
        [deleteTagMutation, refetch, entityRegistry, setShowDeleteModal, setTagToDelete, tagsData],
    );

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
                                    deleteTag(record.entity.urn);
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
        [entityRegistry, searchQuery, sortedInfo, canManageTags, deleteTag],
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

            {/* Delete confirmation modal */}
            {showDeleteModal && tagToDelete && (
                <Modal
                    title={`Delete tag ${tagToDelete.displayName}`}
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
                            onClick: () => {
                                tagToDelete.onConfirm();
                                setShowDeleteModal(false);
                            },
                        },
                    ]}
                >
                    <p>Are you sure you want to delete this tag? This action cannot be undone.</p>
                </Modal>
            )}
        </>
    );
};

export default TagsTable;
