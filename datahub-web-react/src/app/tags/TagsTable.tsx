import React, { useState, useMemo } from 'react';
import { NetworkStatus } from '@apollo/client';
import { Table } from '@components';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { GetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';
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

    // Optimize the tagsData with useMemo to prevent unnecessary filtering on re-renders
    const tagsData = useMemo(() => {
        return searchData?.searchAcrossEntities?.searchResults || [];
    }, [searchData]);

    const [showEdit, setShowEdit] = useState(false);
    const [editingTag, setEditingTag] = useState('');

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
                        />
                    );
                },
            },
        ],
        [entityRegistry, searchQuery, sortedInfo],
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
        </>
    );
};

export default TagsTable;
