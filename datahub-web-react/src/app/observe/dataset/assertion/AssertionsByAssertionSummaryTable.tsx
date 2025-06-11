import { Column, Pagination, Pill, Table, Text, Tooltip } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { titleCase } from '@app/automations/utils';
import { getCronAsText } from '@app/entity/shared/tabs/Dataset/Validations/acrylUtils';
import { getAssertionUrl } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/hooks';
import { getFormattedTimeString } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/utils';
import { AssertionName } from '@app/observe/dataset/assertion/components';
import { ASSERTIONS_DOCS_LINK, RUN_EVENTS_PREVIEW_LIMIT } from '@app/observe/dataset/assertion/constants';
import { healthUrlSuffix } from '@app/previewV2/HealthPopover';
import { getTimeFromNow } from '@app/shared/time/timeUtils';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Assertion, EntityType, HealthStatusType } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: hidden;
`;

const DatasetWrapper = styled(Link)`
    display: flex;
    flex-direction: row;
    gap: 8px;
    align-items: center;
`;

const EmptyState = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 90%;
`;

const ResultsWrapper = styled(Link)`
    display: flex;
    flex-direction: column;
    gap: 6px;
    align-items: flex-start;
`;

const RunDetailsWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    align-items: flex-start;
`;

const getMonitorSchedule = (assertion: Assertion) => {
    return assertion.monitor?.info?.assertionMonitor?.assertions?.find((assrn) => assrn.assertion.urn === assertion.urn)
        ?.schedule;
};

type Props = {
    assertions: Assertion[];
    total: number;
    page: number;
    setPage: (page: number) => void;
    pageSize: number;
    loading: boolean;
    hasModifiedDefaultFilters: boolean;
};
export const AssertionsByAssertionSummaryTable = ({
    assertions,
    total,
    page,
    setPage,
    pageSize,
    loading,
    hasModifiedDefaultFilters,
}: Props) => {
    const entityRegistry = useEntityRegistry();

    const getAllAssertionsLink = (datasetUrn: string) =>
        `${entityRegistry.getEntityUrl(EntityType.Dataset, datasetUrn)}${healthUrlSuffix({ type: HealthStatusType.Assertions })}`.replace(
            '//',
            '/',
        );
    const getAssertionLink = (assertionUrn: string, datasetUrn: string) =>
        getAssertionUrl(assertionUrn, getAllAssertionsLink(datasetUrn));
    const columns: Column<Assertion>[] = [
        {
            key: 'name',
            title: 'Name',
            render: (record) => {
                const monitorSchedule = getMonitorSchedule(record);
                return (
                    <Link to={getAssertionLink(record.urn, record.dataset?.urn || record.monitor?.entity?.urn || '')}>
                        <AssertionName record={record} monitorSchedule={monitorSchedule} />
                    </Link>
                );
            },
        },
        {
            key: 'dataset',
            title: 'Dataset',
            render: (record) => {
                return (
                    <DatasetWrapper to={getAllAssertionsLink(record.dataset?.urn || record.monitor?.entity?.urn || '')}>
                        <PlatformIcon platform={record.dataset?.platform || record.platform} />
                        <Text>
                            {record.dataset
                                ? entityRegistry.getDisplayName(EntityType.Dataset, record.dataset)
                                : 'Unknown'}
                        </Text>
                    </DatasetWrapper>
                );
            },
        },
        {
            key: 'type',
            title: 'Type',
            render: (record) => {
                return <Text>{titleCase(record.info?.type || 'Unknown')}</Text>;
            },
        },
        {
            key: 'results',
            title: 'Results Summary',
            render: (record) => {
                const failures = record.runEvents?.failed || 0;
                const errors = record.runEvents?.errored || 0;
                const passes = record.runEvents?.succeeded || 0;
                const totalResults = record.runEvents?.total || 0;
                const hasMore = totalResults >= RUN_EVENTS_PREVIEW_LIMIT;
                return (
                    <Tooltip
                        title={`This assertion has ${hasMore ? 'more than ' : ''}${totalResults.toLocaleString()} results in the selected time range.`}
                        placement="top"
                    >
                        <ResultsWrapper
                            to={getAssertionLink(record.urn, record.dataset?.urn || record.monitor?.entity?.urn || '')}
                        >
                            {/* ---- Failures ---- */}
                            {failures > 0 ? (
                                <Pill
                                    label={`${failures} failure${failures > 1 ? 's' : ''} ${hasMore ? `+` : ''}`}
                                    color="red"
                                />
                            ) : null}
                            {/* ---- Errors ---- */}
                            {errors > 0 ? (
                                <Pill
                                    label={`${errors} error${errors > 1 ? 's' : ''} ${hasMore ? `+` : ''}`}
                                    color="yellow"
                                />
                            ) : null}
                            {/* ---- Passes ---- */}
                            {passes > 0 ? (
                                <Pill
                                    label={`${passes} pass${passes > 1 ? 'es' : ''} ${hasMore ? `+` : ''}`}
                                    color="green"
                                />
                            ) : null}
                        </ResultsWrapper>
                    </Tooltip>
                );
            },
        },
        {
            key: 'run',
            title: 'Run',
            render: (record) => {
                const lastRun = record.runEvents?.runEvents?.[0]?.timestampMillis;
                const monitorSchedule = getMonitorSchedule(record);
                return (
                    <RunDetailsWrapper>
                        <Tooltip
                            title={lastRun ? getFormattedTimeString(lastRun) : 'This assertion has never been run.'}
                        >
                            <Text weight="semiBold">
                                {lastRun ? `Last run ${getTimeFromNow(lastRun)}` : 'Never run'}
                            </Text>
                        </Tooltip>
                        {monitorSchedule?.cron ? (
                            <Tooltip title={monitorSchedule.cron}>
                                <Text color="gray">Runs {getCronAsText(monitorSchedule.cron).text}</Text>
                            </Tooltip>
                        ) : null}
                    </RunDetailsWrapper>
                );
            },
        },
    ];

    if (total === 0 && !loading) {
        return (
            <Container>
                <EmptyState>
                    <Text size="lg" weight="semiBold">
                        No assertions have run during the filtered time period.
                    </Text>
                    {hasModifiedDefaultFilters && [
                        <Text size="lg" color="gray">
                            Assertions are data quality checks that run automatically to ensure your data is accurate
                            and up to date.
                        </Text>,
                        <a href={ASSERTIONS_DOCS_LINK} target="_blank" rel="noreferrer">
                            <Text size="lg" weight="semiBold">
                                Learn more
                            </Text>
                        </a>,
                    ]}
                </EmptyState>
            </Container>
        );
    }
    return (
        <Container>
            <Table columns={columns} data={assertions} isLoading={loading} isScrollable maxHeight="100%" />
            <Pagination
                currentPage={page}
                totalPages={total}
                itemsPerPage={pageSize}
                onPageChange={(newPage) => setPage(newPage)}
                loading={loading}
            />
        </Container>
    );
};
