import moment from 'moment';
import React from 'react';
import styled from 'styled-components';

import TopUsersFacepile from '@app/entityV2/shared/containers/profile/sidebar/shared/TopUsersFacepile';
import QueryComponent from '@app/entityV2/shared/tabs/Dataset/Queries/Query';
import {
    ColumnsColumn,
    EditDeleteColumn,
    QueryCreatedBy,
    QueryDescription,
} from '@app/entityV2/shared/tabs/Dataset/Queries/queryColumns';
import { Query } from '@app/entityV2/shared/tabs/Dataset/Queries/types';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';
import { Sorting } from '@app/sharedV2/sorting/useSorting';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { CorpUser, Entity } from '@types';

const UsersWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

interface Props {
    queries: Query[];
    hoveredQueryUrn: string | null;
    showDetails?: boolean;
    showEdit?: boolean;
    showDelete?: boolean;
    onDeleted?: (query) => void;
    onEdited?: (query) => void;
    sorting?: Sorting;
    showPagination: boolean;
}

export default function useQueryTableColumns({
    queries,
    hoveredQueryUrn,
    showDetails,
    showEdit,
    showDelete,
    onDeleted,
    onEdited,
    sorting,
    showPagination,
}: Props) {
    const entityRegistry = useEntityRegistryV2();
    // only rely on backend sorting if we provide a sorting config and we are paginating
    const shouldRelyOnBackendSorting = sorting && showPagination;

    const titleColumn = {
        title: 'Title',
        dataIndex: 'title',
        key: 'name',
        field: 'name',
        sorter: shouldRelyOnBackendSorting ? true : (queryA, queryB) => queryA.title?.localeCompare(queryB.title),
        render: (queryTitle: string) => {
            return <div>{queryTitle}</div>;
        },
    };

    const descriptionColumn = {
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        render: (description: string) => <QueryDescription description={description} />,
    };

    const queryTextColumn = (width?: string | number) => ({
        title: 'Query Text',
        dataIndex: 'query',
        key: 'query',
        render: (rowQuery: string) => {
            const query = queries.find(({ query: q }) => q === rowQuery);
            if (!query) return null;
            return (
                <div style={{ width: width || 450 }}>
                    <QueryComponent
                        urn={query.urn}
                        title={query.title || undefined}
                        description={query.description || undefined}
                        query={query.query}
                        createdAtMs={query.createdTime}
                        showDelete={showDelete}
                        showEdit={showEdit}
                        showDetails={showDetails}
                        showHeader={false}
                        onDeleted={() => onDeleted?.(query)}
                        onEdited={(newQuery) => onEdited?.(newQuery)}
                        isCompact
                    />
                </div>
            );
        },
    });

    const createdByColumn = {
        title: 'Created By',
        dataIndex: 'createdBy',
        key: 'createdBy',
        sorter: shouldRelyOnBackendSorting
            ? false // we don't support sorting by createdBy on backend since it is a text field
            : (queryA, queryB) => {
                  if (!queryA.createdBy || !queryB.createdBy) return 0;
                  const createdByA = entityRegistry.getDisplayName(queryA.createdBy.type, queryA.createdBy);
                  const createdByB = entityRegistry.getDisplayName(queryB.createdBy.type, queryB.createdBy);
                  return createdByA.localeCompare(createdByB);
              },
        render: (createdBy: CorpUser) => {
            return <QueryCreatedBy createdBy={createdBy} />;
        },
    };

    const createdDateColumn = {
        title: 'Date Created',
        dataIndex: 'createdTime',
        key: 'dateCreated',
        field: 'createdAt',
        sorter: shouldRelyOnBackendSorting ? true : (queryA, queryB) => queryA.createdTime - queryB.createdTime,
        render: (date: number) => {
            return <div>{moment(date).format('MM/DD/YYYY')}</div>;
        },
    };

    const powersColumn = {
        title: 'Powers',
        dataIndex: 'poweredEntity',
        key: 'powers',
        sorter: (queryA, queryB) => {
            if (!queryA.poweredEntity || !queryB.poweredEntity) return 0;
            const createdByA = entityRegistry.getDisplayName(queryA.poweredEntity.type, queryA.poweredEntity);
            const createdByB = entityRegistry.getDisplayName(queryB.poweredEntity.type, queryB.poweredEntity);
            return createdByA.localeCompare(createdByB);
        },
        render: (entity: Entity) => {
            if (!entity) return null;
            return (
                <div>
                    <EntityLink entity={entity} />
                </div>
            );
        },
    };

    const usedByColumn = {
        title: 'Used By',
        dataIndex: 'usedBy',
        key: 'usedBy',
        className: 'usedBy',
        sorter: shouldRelyOnBackendSorting
            ? false
            : (queryA, queryB) => {
                  if (!queryA.usedBy || !queryA.usedBy[0] || !queryB.usedBy || !queryB.usedBy[0]) return 0;
                  const usedByA = entityRegistry.getDisplayName(queryA.usedBy[0].type, queryA.usedBy[0]);
                  const usedByB = entityRegistry.getDisplayName(queryB.usedBy[0].type, queryB.usedBy[0]);
                  return usedByA.localeCompare(usedByB);
              },
        render: (usedBy: CorpUser[]) => {
            return (
                <UsersWrapper>
                    <TopUsersFacepile users={usedBy} max={3} checkExistence={false} />
                </UsersWrapper>
            );
        },
    };

    const columnsColumn = {
        title: 'Columns',
        key: 'columns',
        width: 105,
        render: (query: Query) => <ColumnsColumn query={query} />,
    };

    const editColumn = {
        title: '',
        key: 'edit',
        width: 80,
        render: (query: Query) => (
            <EditDeleteColumn
                query={query}
                onEdited={onEdited}
                onDeleted={onDeleted}
                hoveredQueryUrn={hoveredQueryUrn}
            />
        ),
    };

    return {
        titleColumn,
        descriptionColumn,
        queryTextColumn,
        createdByColumn,
        createdDateColumn,
        powersColumn,
        usedByColumn,
        columnsColumn,
        editColumn,
    };
}
