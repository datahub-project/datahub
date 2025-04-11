import React, { Dispatch, SetStateAction, useEffect, useMemo, useRef, useState } from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { getTimeFromNow } from '@src/app/shared/time/timeUtils';
import { AssertionResultType, AssertionType } from '@src/types.generated';
import { useHistory, useLocation } from 'react-router';
import { AssertionName } from './AssertionName';
import { getAssertionGroupName } from '../acrylUtils';
import { ActionsColumn } from '../AcrylAssertionsTableColumns';
import { AssertionListFilter } from './types';
import { getQueryParams } from '../assertionUtils';
import { AcrylAssertionTagColumn } from './Tags/AcrylAssertionTagColumn';

const CategoryType = styled.div`
    font-family: Mulish;
    color: ${REDESIGN_COLORS.BODY_TEXT};
    display: flex;
    align-items: center;
    white-space: nowrap;
    width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    font-size: 14px;
`;

const LastRun = styled(Typography.Text)`
    font-family: Mulish;
    color: ${REDESIGN_COLORS.BODY_TEXT};
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
    max-width: 80px;
    display: inline-block;
`;

const TABLE_HEADER_HEIGHT = 50;

export const useAssertionsTableColumns = ({ groupBy, contract, refetch }) => {
    return useMemo(() => {
        const columns = [
            {
                title: 'Name',
                dataIndex: 'name',
                key: 'name',
                render: (record) => <AssertionName record={record} groupBy={groupBy} contract={contract} />,
                width: '42%',
                sorter: (a, b) => {
                    return a - b;
                },
            },
            {
                title: 'Category',
                dataIndex: 'type',
                key: 'type',
                render: (record) =>
                    !record.groupName && <CategoryType>{getAssertionGroupName(record?.type)}</CategoryType>,
                width: '10%',
            },
            {
                title: 'Last Run',
                dataIndex: 'lastEvaluation',
                key: 'lastEvaluation',
                render: (record) => {
                    return !record.groupName && <LastRun>{getTimeFromNow(record.lastEvaluationTimeMs)}</LastRun>;
                },
                width: '10%',
                sorter: (sourceA, sourceB) => {
                    return sourceA.lastEvaluationTimeMs - sourceB.lastEvaluationTimeMs;
                },
            },
            {
                title: 'Tags',
                dataIndex: 'tags',
                key: 'tags',
                width: '20%',
                render: (record) => !record.groupName && <AcrylAssertionTagColumn record={record} refetch={refetch} />,
            },
            {
                title: '',
                dataIndex: '',
                key: 'actions',
                width: '15%',
                render: (record) => {
                    return (
                        !record.groupName && (
                            <ActionsColumn
                                assertion={record.assertion}
                                contract={contract}
                                canEditContract
                                refetch={refetch}
                                shouldRightAlign
                                options={{ removeRightPadding: true }}
                            />
                        )
                    );
                },
            },
        ];

        return columns;
    }, [groupBy, contract, refetch]);
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

/** set filter as per the params we are getting from URL set assertion type and status as per the url */
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
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [assertionType, assertionStatus, location, history]);

    return { filter };
};
