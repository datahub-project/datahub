import { Column } from '@components';
import { Tooltip, Typography } from 'antd';
import React, { Dispatch, SetStateAction, useCallback, useEffect, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import { ActionsColumn } from '@app/entityV2/shared/tabs/Dataset/Validations/AcrylAssertionsTableColumns';
import { AcrylAssertionOwnerColumn } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionOwnerColumn';
import { AssertionName } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AssertionName';
import { AcrylAssertionTagColumn } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/Tags/AcrylAssertionTagColumn';
import {
    AssertionListFilter,
    AssertionListTableRow,
} from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { getAssertionGroupName } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { getQueryParams } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';
import { getTimeFromNow } from '@src/app/shared/time/timeUtils';
import { AssertionResultType, AssertionType, DataContract } from '@src/types.generated';
import dayjs from '@utils/dayjs';

const CategoryType = styled.div`
    font-family: Mulish;
    color: ${(props) => props.theme.colors.text};
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
    color: ${(props) => props.theme.colors.text};
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
    max-width: 120px;
    display: inline-block;
    font-size: 14px;
`;
const DEFAULT_DATETIME_FORMAT = 'l @ LT (z)';

export const useAssertionsTableColumns = ({
    contract,
    refetch,
}: {
    contract: DataContract | undefined;
    refetch: () => void;
}) => {
    const { t } = useTranslation('entity.profile.validations');
    const { t: tl } = useTranslation('common.labels');
    const renderAssertionName = useCallback(
        (record: AssertionListTableRow) => (
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
        (record: AssertionListTableRow) =>
            !record.groupName &&
            record?.type && <CategoryType key={record.urn}>{getAssertionGroupName(record.type)}</CategoryType>,
        [],
    );

    const renderLastRun = useCallback(
        (record: AssertionListTableRow) =>
            !record.groupName && (
                <Tooltip placement="topLeft" title={dayjs(record.lastEvaluationTimeMs).format(DEFAULT_DATETIME_FORMAT)}>
                    <LastRun key={record.urn}>{getTimeFromNow(record.lastEvaluationTimeMs)}</LastRun>
                </Tooltip>
            ),
        [],
    );

    const renderOwners = useCallback(
        (record: AssertionListTableRow) =>
            !record.groupName && <AcrylAssertionOwnerColumn key={record.urn} record={record} refetch={refetch} />,
        [refetch],
    );

    const renderTags = useCallback(
        (record: AssertionListTableRow) =>
            !record.groupName && <AcrylAssertionTagColumn key={record.urn} record={record} refetch={refetch} />,
        [refetch],
    );

    const renderActions = useCallback(
        (record: AssertionListTableRow) =>
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
            ),
        [contract, refetch],
    );

    return useMemo(() => {
        const columns: Column<AssertionListTableRow>[] = [
            {
                title: tl('name'),
                dataIndex: 'name',
                key: 'name',
                render: renderAssertionName,
                width: '40%',
            },
            {
                title: tl('category'),
                dataIndex: 'type',
                key: 'type',
                render: renderCategory,
                width: '12%',
                sorter: true,
            },
            {
                title: t('column.lastRun'),
                dataIndex: 'lastEvaluation',
                key: 'lastEvaluation',
                render: renderLastRun,
                width: '15%',
                sorter: true,
            },
            {
                title: tl('owners'),
                dataIndex: 'ownership',
                key: 'owners',
                width: '10%',
                render: renderOwners,
            },
            {
                title: tl('tags'),
                dataIndex: 'tags',
                key: 'tags',
                width: '13%',
                render: renderTags,
            },
            {
                title: '',
                dataIndex: '',
                key: 'actions',
                width: '10%',
                render: renderActions,
                alignment: 'right',
            },
        ];

        return columns;
    }, [t, tl, renderAssertionName, renderCategory, renderLastRun, renderOwners, renderTags, renderActions]);
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
