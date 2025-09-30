import { Typography } from 'antd';
import { ColumnType } from 'antd/lib/table';
import React, { Dispatch, SetStateAction, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import { ActionsColumn } from '@app/entityV2/shared/tabs/Dataset/Validations/AcrylAssertionsTableColumns';
import { AssertionName } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AssertionName';
import { AcrylAssertionTagColumn } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/Tags/AcrylAssertionTagColumn';
import {
    AssertionListFilter,
    AssertionListTableRow,
} from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { getAssertionGroupName } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { getQueryParams } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { getTimeFromNow } from '@src/app/shared/time/timeUtils';
import { AssertionResultType, AssertionType } from '@src/types.generated';

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

export const useAssertionsTableColumns = ({ contract, refetch }) => {
    const renderAssertionName = useCallback(
        (_, record) => (
            <AssertionName
                key={record.urn}
                assertion={record.assertion}
                lastEvaluation={record.lastEvaluation}
                lastEvaluationUrl={record.lastEvaluationUrl}
                platform={record.platform}
                contract={contract}
            />
        ),
        [contract],
    );

    const renderCategory = useCallback(
        (_, record) =>
            !record.groupName &&
            record?.type && <CategoryType key={record.urn}>{getAssertionGroupName(record.type)}</CategoryType>,
        [],
    );

    const renderLastRun = useCallback(
        (_, record) =>
            !record.groupName && <LastRun key={record.urn}>{getTimeFromNow(record.lastEvaluationTimeMs)}</LastRun>,
        [],
    );

    const renderTags = useCallback(
        (_, record) =>
            !record.groupName && <AcrylAssertionTagColumn key={record.urn} record={record} refetch={refetch} />,
        [refetch],
    );

    const renderActions = useCallback(
        (_, record) => {
            return (
                !record.groupName && (
                    <ActionsColumn
                        key={record.urn}
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
        [contract, refetch],
    );

    return useMemo(() => {
        const columns: ColumnType<AssertionListTableRow>[] = [
            {
                title: 'Name',
                dataIndex: 'name',
                key: 'name',
                render: renderAssertionName,
                width: '45%',
                sorter: (a, b) => {
                    return a.description.localeCompare(b.description);
                },
                ellipsis: {
                    showTitle: false,
                },
            },
            {
                title: 'Category',
                dataIndex: 'type',
                key: 'type',
                render: renderCategory,
                width: '12%',
                sorter: (a, b) => {
                    if (a.type && b.type) {
                        return getAssertionGroupName(a.type).localeCompare(getAssertionGroupName(b.type));
                    }
                    return 0;
                },
                ellipsis: {
                    showTitle: false,
                },
            },
            {
                title: 'Last Run',
                dataIndex: 'lastEvaluation',
                key: 'lastEvaluation',
                render: renderLastRun,
                width: '15%',
                sorter: (sourceA, sourceB) => {
                    if (!sourceA.lastEvaluationTimeMs || !sourceB.lastEvaluationTimeMs) {
                        return 0;
                    }
                    return sourceA.lastEvaluationTimeMs - sourceB.lastEvaluationTimeMs;
                },
                defaultSortOrder: 'descend',
                ellipsis: {
                    showTitle: false,
                },
            },
            {
                title: 'Tags',
                dataIndex: 'tags',
                key: 'tags',
                width: '18%',
                render: renderTags,
                ellipsis: {
                    showTitle: false,
                },
            },
            {
                title: '',
                dataIndex: '',
                key: 'actions',
                width: '10%',
                render: renderActions,
                fixed: 'right',
            },
        ];

        return columns;
    }, [renderAssertionName, renderCategory, renderLastRun, renderTags, renderActions]);
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
