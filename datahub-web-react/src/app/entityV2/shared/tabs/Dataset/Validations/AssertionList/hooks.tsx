import * as QueryString from 'query-string';
import React, { Dispatch, SetStateAction, useEffect, useMemo, useRef, useState } from 'react';
import { Table, TableColumnsType, Typography } from 'antd';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { getTimeFromNow } from '@src/app/shared/time/timeUtils';
import { AssertionResultType, AssertionType } from '@src/types.generated';
import { AssertionName } from './AssertionName';
import { getAssertionGroupName } from '../acrylUtils';
import { ActionsColumn } from '../AcrylAssertionsTableColumns';
import { useHistory, useLocation } from 'react-router';
import { AssertionListFilter } from './types';
import { getQueryParams } from '../assertionUtils';

const CategoryType = styled.div`
    font-family: Mulish;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;

const LastRun = styled(Typography.Text)`
    font-family: Mulish;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;

const TABLE_HEADER_HEIGHT = 50;

export const useAssertionsTableColumns = ({
    groupBy,
    contract,
    canEditSqlAssertions,
    canEditAssertions,
    canEditMonitors,
    refetch,
}) => {
    return useMemo(() => {
        const columns: TableColumnsType<any> = [
            {
                title: 'Name',
                dataIndex: 'name',
                key: 'name',
                render: (_, record) => <AssertionName record={record} groupBy={groupBy} contract={contract} />,
                width: '50%',
                sorter: (a, b) => a.description?.localeCompare(b.description),
            },
            {
                title: 'Category',
                dataIndex: 'type',
                key: 'type',
                render: (_, record) =>
                    !record.groupName && <CategoryType>{getAssertionGroupName(record?.type)}</CategoryType>,
                sorter: (a, b) => a.type?.localeCompare(b.type),
                width: '11%',
            },
            {
                title: 'Last Run',
                dataIndex: 'lastEvaluation',
                key: 'lastEvaluation',
                render: (_, record) => {
                    return !record.groupName && <LastRun>{getTimeFromNow(record.lastEvaluationTimeMs)}</LastRun>;
                },
                sorter: (a, b) => (a.lastEvaluationTimeMs || 0) - (b.lastEvaluationTimeMs || 0),
                width: '12%',
            },
            {
                title: 'Tags',
                dataIndex: 'tags',
                key: 'tags',
                width: '12%',
                render: (_, record) => <div>{record.tags?.name}</div>,
            },
            {
                title: '',
                dataIndex: '',
                key: 'actions',
                width: '10%',
                render: (_, record) => {
                    const isSqlAssertion = record.type === AssertionType.Sql;
                    return (
                        !record.groupName && (
                            <ActionsColumn
                                assertion={record.assertion}
                                monitor={record.monitor}
                                contract={contract}
                                canEditAssertion={isSqlAssertion ? canEditSqlAssertions : canEditAssertions}
                                canEditMonitor={canEditMonitors}
                                canEditContract
                                refetch={refetch}
                            />
                        )
                    );
                },
            },
        ];

        if (groupBy) {
            columns.push(Table.EXPAND_COLUMN);
        }

        return columns;
    }, [groupBy, contract, canEditSqlAssertions, canEditAssertions, canEditMonitors, refetch]);
};

export const usePinnedAssertionTableHeaderProps = () => {
    // Dynamic height calculation
    const tableContainerRef = useRef<HTMLDivElement>(null);
    const [scrollY, setScrollY] = useState<number>(0);

    useEffect(() => {
        const handleResize = () => {
            if (tableContainerRef.current) {
                const containerHeight = tableContainerRef.current.getBoundingClientRect().height;
                setScrollY(containerHeight - TABLE_HEADER_HEIGHT);
            }
        };

        handleResize();
        window.addEventListener('resize', handleResize);

        return () => {
            window.removeEventListener('resize', handleResize);
        };
    }, []);

    return { tableContainerRef, scrollY };
};

export const useSetFilterFromURLParams = (
    filter: AssertionListFilter,
    setFilters: Dispatch<SetStateAction<AssertionListFilter>>,
) => {
    const location = useLocation();
    const history = useHistory();
    const assertionType = getQueryParams('assertion_type', location);
    const assertionStatus = getQueryParams('assertion_status', location);

    useEffect(() => {
        if (assertionType || assertionStatus) {
            const decodedAssertionType = decodeURIComponent(assertionType || '');
            const decodedAssertionStatus = decodeURIComponent(assertionStatus || '');

            const updatedFilterCriteria = { ...filter.filterCriteria };
            if (decodedAssertionType) {
                updatedFilterCriteria.type = [decodedAssertionType as AssertionType];
            }
            if (decodedAssertionStatus) {
                updatedFilterCriteria.status = [decodedAssertionStatus as AssertionResultType];
            }

            const newUrlParams = new URLSearchParams(location.search);
            newUrlParams.delete('assertion_type');
            newUrlParams.delete('assertion_status');
            const newUrl = `${location.pathname}?${newUrlParams.toString()}`;

            if (assertionType || assertionStatus) {
                setFilters({ ...filter, filterCriteria: updatedFilterCriteria });
            }

            history.replace(newUrl);
        }
    }, [assertionType, assertionStatus, location, history]);

    return { filter };
};
