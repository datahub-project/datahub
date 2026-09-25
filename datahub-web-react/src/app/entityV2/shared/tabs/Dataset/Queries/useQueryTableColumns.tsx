import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Column } from '@components/components/Table/types';

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
import dayjs from '@utils/dayjs';

import { CorpUser, Entity } from '@types';

const DATE_FORMAT = 'MM/DD/YYYY';

function compareNullableNumbers(a?: number | null, b?: number | null): number {
    return (a ?? 0) - (b ?? 0);
}

const UsersWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

interface Props {
    showDetails?: boolean;
    onDeleted?: (query) => void;
    onEdited?: (query) => void;
    sorting?: Sorting;
    showPagination: boolean;
}

export default function useQueryTableColumns({ showDetails, onDeleted, onEdited, sorting, showPagination }: Props) {
    const { t } = useTranslation('entity.profile.queries');
    const { t: tc } = useTranslation('common.labels');
    const entityRegistry = useEntityRegistryV2();
    // only rely on backend sorting if we provide a sorting config and we are paginating
    const shouldRelyOnBackendSorting = sorting && showPagination;

    const titleColumn: Column<Query> = {
        title: tc('title'),
        key: 'name',
        sorter: shouldRelyOnBackendSorting
            ? true
            : (queryA, queryB) => (queryA.title ?? '').localeCompare(queryB.title ?? ''),
        render: (query) => {
            return <div>{query.title}</div>;
        },
    };

    const descriptionColumn: Column<Query> = {
        title: t('queryCard.columnDescription'),
        key: 'description',
        minWidth: '240px',
        render: (query) => <QueryDescription description={query.description} />,
    };

    const queryTextColumn = (width = '450px'): Column<Query> => ({
        title: t('queryCard.columnQueryText'),
        key: 'query',
        width,
        render: (query) => {
            return (
                <div style={{ width }}>
                    <QueryComponent
                        title={query.title || undefined}
                        description={query.description || undefined}
                        query={query.query}
                        createdAtMs={query.createdTime}
                        showDetails={showDetails}
                        showHeader={false}
                        isCompact
                    />
                </div>
            );
        },
    });

    const createdByColumn: Column<Query> = {
        title: t('queryCard.columnCreatedBy'),
        key: 'createdBy',
        width: '140px',
        sorter: shouldRelyOnBackendSorting
            ? false // we don't support sorting by createdBy on backend since it is a text field
            : (queryA, queryB) => {
                  if (!queryA.createdBy || !queryB.createdBy) return 0;
                  const createdByA = entityRegistry.getDisplayName(queryA.createdBy.type, queryA.createdBy);
                  const createdByB = entityRegistry.getDisplayName(queryB.createdBy.type, queryB.createdBy);
                  return createdByA.localeCompare(createdByB);
              },
        render: (query) => {
            return <QueryCreatedBy createdBy={query.createdBy ?? undefined} />;
        },
    };

    const createdDateColumn: Column<Query> = {
        title: t('queryCard.columnDateCreated'),
        key: 'dateCreated',
        sorter: shouldRelyOnBackendSorting
            ? true
            : (queryA, queryB) => compareNullableNumbers(queryA.createdTime, queryB.createdTime),
        render: (query) => {
            return <div>{dayjs(query.createdTime).format(DATE_FORMAT)}</div>;
        },
    };

    const powersColumn: Column<Query> = {
        title: t('queryCard.columnPowers'),
        key: 'powers',
        sorter: (queryA, queryB) => {
            if (!queryA.poweredEntity || !queryB.poweredEntity) return 0;
            const createdByA = entityRegistry.getDisplayName(queryA.poweredEntity.type, queryA.poweredEntity);
            const createdByB = entityRegistry.getDisplayName(queryB.poweredEntity.type, queryB.poweredEntity);
            return createdByA.localeCompare(createdByB);
        },
        render: (query) => {
            const entity = query.poweredEntity as Entity;
            if (!entity) return null;
            return (
                <div>
                    <EntityLink entity={entity} />
                </div>
            );
        },
    };

    const topUsersColumn: Column<Query> = {
        title: t('queryCard.columnTopUsers'),
        key: 'usedBy',
        minWidth: '100px',
        sorter: shouldRelyOnBackendSorting
            ? false
            : (queryA, queryB) => {
                  if (!queryA.usedBy || !queryA.usedBy[0] || !queryB.usedBy || !queryB.usedBy[0]) return 0;
                  const usedByA = entityRegistry.getDisplayName(queryA.usedBy[0].type, queryA.usedBy[0]);
                  const usedByB = entityRegistry.getDisplayName(queryB.usedBy[0].type, queryB.usedBy[0]);
                  return usedByA.localeCompare(usedByB);
              },
        render: (query) => {
            return (
                <UsersWrapper>
                    <TopUsersFacepile users={query.usedBy as CorpUser[]} max={3} checkExistence={false} />
                </UsersWrapper>
            );
        },
    };

    const columnsColumn: Column<Query> = {
        title: tc('columns'),
        key: 'columns',
        width: '105px',
        render: (query) => <ColumnsColumn query={query} />,
    };

    const editColumn: Column<Query> = {
        title: '',
        key: 'edit',
        width: '80px',
        alignment: 'right',
        render: (query) => <EditDeleteColumn query={query} onEdited={onEdited} onDeleted={onDeleted} />,
    };

    return {
        titleColumn,
        descriptionColumn,
        queryTextColumn,
        createdByColumn,
        createdDateColumn,
        powersColumn,
        topUsersColumn,
        columnsColumn,
        editColumn,
    };
}
