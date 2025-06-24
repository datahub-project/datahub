import { Column, Pagination, Pill, Table, Text, Tooltip, colors } from '@components';
import { Sparkle } from 'phosphor-react';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { getCronAsText } from '@app/entity/shared/tabs/Dataset/Validations/acrylUtils';
import { getAssertionUrl } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/hooks';
import { getFormattedTimeString } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/utils';
import { AssertionName } from '@app/observe/dataset/assertion/components';
import {
    ASSERTIONS_DOCS_LINK,
    RUN_EVENTS_PREVIEW_LIMIT,
    TYPE_TO_DISPLAY_NAME,
} from '@app/observe/dataset/assertion/constants';
import { healthUrlSuffix } from '@app/previewV2/HealthPopover';
import { getTimeFromNow } from '@app/shared/time/timeUtils';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Assertion, AssertionSourceType, EntityType, HealthStatusType } from '@types';

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
    &:hover {
        text-decoration: underline;
    }
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
    flex-direction: row;
    gap: 4px;
    align-items: flex-start;
`;

const RunDetailsWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 0;
    align-items: flex-start;
    max-width: 300px;
`;

const TypeWrapper = styled.div`
    display: flex;
    flex-direction: row;
    gap: 4px;
    align-items: center;
`;

const LastRunText = styled(Text)`
    color: ${colors.gray[400]};
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const getMonitorSchedule = (assertion: Assertion) => {
    return assertion.monitor?.info?.assertionMonitor?.assertions?.find((assrn) => assrn.assertion.urn === assertion.urn)
        ?.schedule;
};

const roundToNearest = (num: number) => {
    if (num < 100) {
        return Math.round(num / 10) * 10;
    }
    return Math.round(num / 100) * 100;
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
                    <Link
                        to={getAssertionLink(record.urn, record.dataset?.urn || record.monitor?.entity?.urn || '')}
                        onClick={() => {
                            analytics.event({
                                type: EventType.DatasetHealthClickEvent,
                                tabType: 'AssertionsByAssertion',
                                target: 'assertion',
                                targetUrn: record.urn,
                            });
                        }}
                    >
                        <AssertionName record={record} monitorSchedule={monitorSchedule} />
                    </Link>
                );
            },
        },
        {
            key: 'type',
            title: 'Type',
            render: (record) => {
                return (
                    <TypeWrapper>
                        <Text>{TYPE_TO_DISPLAY_NAME.get(record.info?.type) || 'Unknown'}</Text>
                        {record.info?.source?.type === AssertionSourceType.Inferred ? (
                            <Tooltip title="Smart assertion (AI anomaly detection)">
                                <Sparkle size={16} color={colors.blue[500]} />
                            </Tooltip>
                        ) : null}
                    </TypeWrapper>
                );
            },
        },
        {
            key: 'dataset',
            title: 'Dataset',
            maxWidth: '200px',
            render: (record) => {
                return (
                    <DatasetWrapper
                        to={getAllAssertionsLink(record.dataset?.urn || record.monitor?.entity?.urn || '')}
                        onClick={() => {
                            analytics.event({
                                type: EventType.DatasetHealthClickEvent,
                                tabType: 'AssertionsByAssertion',
                                target: 'asset_assertions',
                                targetUrn: record.dataset?.urn || record.monitor?.entity?.urn || '',
                            });
                        }}
                        color="gray"
                    >
                        <PlatformIcon platform={record.dataset?.platform || record.platform} />
                        <Text
                            color="gray"
                            size="md"
                            weight="medium"
                            style={{
                                maxWidth: '150px',
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                            }}
                        >
                            {record.dataset
                                ? entityRegistry.getDisplayName(EntityType.Dataset, record.dataset)
                                : 'Unknown'}
                        </Text>
                    </DatasetWrapper>
                );
            },
        },
        {
            key: 'results',
            title: 'Results',
            render: (record) => {
                const failures = record.runEvents?.failed || 0;
                const errors = record.runEvents?.errored || 0;
                const passes = record.runEvents?.succeeded || 0;
                const totalResults = record.runEvents?.total || 0;
                const hasMore = totalResults >= RUN_EVENTS_PREVIEW_LIMIT;

                const failuresToDisplay = hasMore ? `${roundToNearest(failures)}+` : failures.toString();
                const errorsToDisplay = hasMore ? `${roundToNearest(errors)}+` : errors.toString();
                const passesToDisplay = hasMore ? `${roundToNearest(passes)}+` : passes.toString();
                return (
                    <ResultsWrapper
                        to={getAssertionLink(record.urn, record.dataset?.urn || record.monitor?.entity?.urn || '')}
                        onClick={() => {
                            analytics.event({
                                type: EventType.DatasetHealthClickEvent,
                                tabType: 'AssertionsByAssertion',
                                target: 'assertion',
                                subTarget: 'results',
                                targetUrn: record.urn,
                            });
                        }}
                    >
                        {/* ---- Failures ---- */}
                        {failures > 0 ? (
                            <Tooltip
                                title={
                                    hasMore
                                        ? `Cannot fetch all failures in this time period. Click to see all results.`
                                        : `${failuresToDisplay} failure${failures > 1 ? 's' : ''} in this time period`
                                }
                                placement="top"
                            >
                                {/* NOTE: we have to wrap the pill in a div to avoid the tooltip from being cut off */}
                                <div>
                                    <Pill label={failuresToDisplay} color="red" leftIcon="X" iconSource="phosphor" />
                                </div>
                            </Tooltip>
                        ) : null}
                        {/* ---- Errors ---- */}
                        {errors > 0 ? (
                            <Tooltip
                                title={
                                    hasMore
                                        ? `Cannot fetch all errors in this time period. Click to see all results.`
                                        : `${errorsToDisplay} error${errors > 1 ? 's' : ''} in this time period`
                                }
                                placement="top"
                            >
                                {/* NOTE: we have to wrap the pill in a div to avoid the tooltip from being cut off */}
                                <div>
                                    <Pill
                                        label={errorsToDisplay}
                                        color="yellow"
                                        leftIcon="Warning"
                                        iconSource="phosphor"
                                    />
                                </div>
                            </Tooltip>
                        ) : null}
                        {/* ---- Passes ---- */}
                        {passes > 0 ? (
                            <Tooltip
                                title={
                                    hasMore
                                        ? `Cannot fetch all passes in this time period. Click to see all results.`
                                        : `${passesToDisplay} pass${passes > 1 ? 'es' : ''} in this time period`
                                }
                                placement="top"
                            >
                                {/* NOTE: we have to wrap the pill in a div to avoid the tooltip from being cut off */}
                                <div>
                                    <Pill
                                        label={passesToDisplay}
                                        color="green"
                                        leftIcon="Check"
                                        iconSource="phosphor"
                                    />
                                </div>
                            </Tooltip>
                        ) : null}
                    </ResultsWrapper>
                );
            },
        },
        {
            key: 'lastrun',
            title: 'Last Run',
            sorter: true,
            render: (record) => {
                const lastRun = record.runEvents?.runEvents?.[0]?.timestampMillis;
                const monitorSchedule = getMonitorSchedule(record);
                return (
                    <RunDetailsWrapper>
                        <Tooltip
                            title={
                                lastRun
                                    ? `Last run at ${getFormattedTimeString(lastRun)}`
                                    : 'This assertion has never been run.'
                            }
                        >
                            <Text weight="semiBold">{lastRun ? getTimeFromNow(lastRun) : 'never run'}</Text>
                        </Tooltip>
                        {monitorSchedule?.cron ? (
                            <Tooltip title={monitorSchedule.cron}>
                                <LastRunText>runs {getCronAsText(monitorSchedule.cron).text}</LastRunText>
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
                total={total}
                itemsPerPage={pageSize}
                onPageChange={(newPage) => setPage(newPage)}
                loading={loading}
            />
        </Container>
    );
};
