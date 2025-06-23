import { Column, Pagination, Table, Text, Tooltip, colors } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { formatTimestamp } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/utils';
import { buildAssertionUrlSearch } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';
import { renderOwners } from '@app/observe/dataset/shared/shared';
import ContextPath from '@app/previewV2/ContextPath';
import { healthUrlSuffix } from '@app/previewV2/HealthPopover';
import { getParentEntities } from '@app/searchV2/filters/utils';
import { getTimeFromNow } from '@app/shared/time/timeUtils';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';
import { useEntityRegistry } from '@app/useEntityRegistry';

import {
    AssertionHealthStatusByType,
    AssertionType,
    Dataset,
    EntityType,
    Health,
    HealthStatus,
    HealthStatusType,
    Maybe,
} from '@types';

const DatasetNameColumn = styled(Link)`
    display: flex;
    align-items: center;
    gap: 8px;
    &:hover {
        text-decoration: underline;
    }
    margin-left: 8px;
`;

const DatasetNameColumnWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    flex-direction: row;
`;

const Container = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: hidden;
`;

const HealthStatusToColor = {
    [HealthStatus.Pass]: colors.green[500],
    [HealthStatus.Fail]: colors.red[500],
    [HealthStatus.Warn]: colors.yellow[500],
};
const HealthStatusToBackgroundColor = {
    [HealthStatus.Pass]: colors.green[100],
    [HealthStatus.Fail]: colors.red[100],
    [HealthStatus.Warn]: colors.yellow[100],
};

const HealthStatusToText = {
    [HealthStatus.Pass]: 'passing',
    [HealthStatus.Fail]: 'failing',
    [HealthStatus.Warn]: 'warning',
    DEFAULT: 'pending run',
};

const HealthStatusIndicator = styled.div<{ status?: HealthStatus }>`
    width: 16px;
    height: 16px;
    border-radius: 100%;
    align-self: center;
    border: 4px solid ${(props) => (props.status ? HealthStatusToBackgroundColor[props.status] : colors.gray[100])};
    background-color: ${(props) => (props.status ? HealthStatusToColor[props.status] : colors.gray[200])};
`;

const HealthStatusIndicatorLink = styled(Link)`
    display: flex;
    align-items: center;
    justify-content: center;
    padding-right: 8px;
`;

const getAssertionHealth = (entityHealth?: Maybe<Health[]>): Health | undefined => {
    return entityHealth?.find((h) => h.type === HealthStatusType.Assertions);
};
const getHealthForAssertionType = (
    assertionType: AssertionType,
    assertionHealth?: Health,
): AssertionHealthStatusByType | undefined => {
    return assertionHealth?.latestAssertionStatusByType?.find((h) => h.type === assertionType);
};

const checkHasAssertionsOfType = (assertionType: AssertionType, datasets: Dataset[]): boolean => {
    return datasets.some((dataset) =>
        dataset.health?.some(
            (h) =>
                h.type === HealthStatusType.Assertions &&
                h.latestAssertionStatusByType?.some((status) => status.type === assertionType),
        ),
    );
};

type Props = {
    datasets: Dataset[];
    isLoading: boolean;
    page: number;
    setPage: (page: number) => void;
    pageSize: number;
    total: number;
};
export const AssertionsByTableSummaryTable = ({ datasets, isLoading, page, setPage, pageSize, total }: Props) => {
    const entityRegistry = useEntityRegistry();

    const hasCustomAssertions = checkHasAssertionsOfType(AssertionType.Custom, datasets);
    const hasExternalAssertions = checkHasAssertionsOfType(AssertionType.Dataset, datasets);

    const getAssertionsLink = (record: Dataset) =>
        `${entityRegistry.getEntityUrl(EntityType.Dataset, record.urn)}${healthUrlSuffix({ type: HealthStatusType.Assertions })}`;

    /* ------------************************* Columns Definition *************************------------ */
    const columns: Column<Dataset>[] = [
        /* ************************* Informational Columns ************************* */
        {
            key: 'name',
            title: 'Name',
            render: (record) => {
                const contextPath = getParentEntities(record);
                return (
                    <DatasetNameColumnWrapper>
                        <PlatformIcon platform={record.platform} />
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
                            <DatasetNameColumn
                                to={getAssertionsLink(record)}
                                data-testid={`preview-${record.urn}`}
                                onClick={() => {
                                    analytics.event({
                                        type: EventType.DatasetHealthClickEvent,
                                        tabType: 'AssertionsByAsset',
                                        target: 'asset_assertions',
                                        targetUrn: record.urn,
                                    });
                                }}
                            >
                                <Text color="black" weight="normal">
                                    {record.name}
                                </Text>
                            </DatasetNameColumn>
                            <ContextPath
                                entityType={EntityType.Dataset}
                                parentEntities={contextPath}
                                browsePaths={record.browsePathV2}
                                entityTitleWidth={150}
                                isCompactView
                                hideTypeIcons
                            />
                        </div>
                    </DatasetNameColumnWrapper>
                );
            },
        },
        {
            key: 'domain',
            title: 'Domain',
            render: (record) => {
                return <div>{record.domain?.domain && <DomainLink domain={record.domain.domain} />}</div>;
            },
        },
        {
            key: 'owners',
            title: 'Owners',
            render: (record) => {
                return renderOwners(record.ownership?.owners ?? [], entityRegistry);
            },
        },

        /* ************************* Status Columns ************************* */
        {
            key: 'freshness',
            title: 'Freshness',
            width: '80px',
            render: (record) => {
                const assertionHealth = getAssertionHealth(record.health);
                const freshnessHealth = getHealthForAssertionType(AssertionType.Freshness, assertionHealth);
                const message = freshnessHealth
                    ? `${freshnessHealth.statusCount} ${freshnessHealth.statusCount === 1 ? 'is' : 'are'} ${HealthStatusToText[freshnessHealth.status]} (${freshnessHealth.total} total assertions)`
                    : // TODO: link to asset page, quality tab with auto-trigger create assertion modal in query params
                      'No freshness assertions have run yet. Create one to see results here.';
                return (
                    <Tooltip title={message}>
                        <HealthStatusIndicatorLink
                            to={`${getAssertionsLink(record)}${buildAssertionUrlSearch({ type: AssertionType.Freshness })}`}
                            onClick={() => {
                                analytics.event({
                                    type: EventType.DatasetHealthClickEvent,
                                    tabType: 'AssertionsByAsset',
                                    target: 'asset_assertions',
                                    subTarget: 'freshness_results',
                                    targetUrn: record.urn,
                                });
                            }}
                        >
                            <HealthStatusIndicator status={freshnessHealth?.status} />
                        </HealthStatusIndicatorLink>
                    </Tooltip>
                );
            },
        },
        {
            key: 'volume',
            title: 'Volume',
            width: '80px',
            render: (record) => {
                const assertionHealth = getAssertionHealth(record.health);
                const volumeHealth = getHealthForAssertionType(AssertionType.Volume, assertionHealth);
                const message = volumeHealth
                    ? `${volumeHealth.statusCount} ${volumeHealth.statusCount === 1 ? 'is' : 'are'} ${HealthStatusToText[volumeHealth.status]} (${volumeHealth.total} total assertions)`
                    : // TODO: link to asset page, quality tab with auto-trigger create assertion modal in query params
                      'No volume assertions have run yet. Create one to see results here.';
                return (
                    <Tooltip title={message}>
                        <HealthStatusIndicatorLink
                            to={`${getAssertionsLink(record)}${buildAssertionUrlSearch({ type: AssertionType.Volume })}`}
                            onClick={() => {
                                analytics.event({
                                    type: EventType.DatasetHealthClickEvent,
                                    tabType: 'AssertionsByAsset',
                                    target: 'asset_assertions',
                                    subTarget: 'volume_results',
                                    targetUrn: record.urn,
                                });
                            }}
                        >
                            <HealthStatusIndicator status={volumeHealth?.status} />
                        </HealthStatusIndicatorLink>
                    </Tooltip>
                );
            },
        },

        {
            key: 'field',
            title: 'Column',
            width: '80px',
            render: (record) => {
                const assertionHealth = getAssertionHealth(record.health);
                const fieldHealth = getHealthForAssertionType(AssertionType.Field, assertionHealth);
                const message = fieldHealth
                    ? `${fieldHealth.statusCount} ${fieldHealth.statusCount === 1 ? 'is' : 'are'} ${HealthStatusToText[fieldHealth.status]} (${fieldHealth.total} total assertions)`
                    : // TODO: link to asset page, quality tab with auto-trigger create assertion modal in query params
                      'No field assertions have run yet. Create one to see results here.';
                return (
                    <Tooltip title={message}>
                        <HealthStatusIndicatorLink
                            to={`${getAssertionsLink(record)}${buildAssertionUrlSearch({ type: AssertionType.Field })}`}
                            onClick={() => {
                                analytics.event({
                                    type: EventType.DatasetHealthClickEvent,
                                    tabType: 'AssertionsByAsset',
                                    target: 'asset_assertions',
                                    subTarget: 'field_results',
                                    targetUrn: record.urn,
                                });
                            }}
                        >
                            <HealthStatusIndicator status={fieldHealth?.status} />
                        </HealthStatusIndicatorLink>
                    </Tooltip>
                );
            },
        },
        {
            key: 'schema',
            title: 'Schema',
            width: '80px',
            render: (record) => {
                const assertionHealth = getAssertionHealth(record.health);
                const schemaHealth = getHealthForAssertionType(AssertionType.DataSchema, assertionHealth);
                const message = schemaHealth
                    ? `${schemaHealth.statusCount} ${schemaHealth.statusCount === 1 ? 'is' : 'are'} ${HealthStatusToText[schemaHealth.status]} (${schemaHealth.total} total assertions)`
                    : // TODO: link to asset page, quality tab with auto-trigger create assertion modal in query params
                      'No schema assertions have run yet. Create one to see results here.';
                return (
                    <Tooltip title={message}>
                        <HealthStatusIndicatorLink
                            to={`${getAssertionsLink(record)}${buildAssertionUrlSearch({ type: AssertionType.DataSchema })}`}
                            onClick={() => {
                                analytics.event({
                                    type: EventType.DatasetHealthClickEvent,
                                    tabType: 'AssertionsByAsset',
                                    target: 'asset_assertions',
                                    subTarget: 'schema_results',
                                    targetUrn: record.urn,
                                });
                            }}
                        >
                            <HealthStatusIndicator status={schemaHealth?.status} />
                        </HealthStatusIndicatorLink>
                    </Tooltip>
                );
            },
        },
        {
            key: 'sql',
            title: 'SQL',
            width: '80px',
            render: (record) => {
                const assertionHealth = getAssertionHealth(record.health);
                const sqlHealth = getHealthForAssertionType(AssertionType.Sql, assertionHealth);
                const message = sqlHealth
                    ? `${sqlHealth.statusCount} ${sqlHealth.statusCount === 1 ? 'is' : 'are'} ${HealthStatusToText[sqlHealth.status]} (${sqlHealth.total} total assertions)`
                    : // TODO: link to asset page, quality tab with auto-trigger create assertion modal in query params
                      'No SQL assertions have run yet. Create one to see results here.';
                return (
                    <Tooltip title={message}>
                        <HealthStatusIndicatorLink
                            to={`${getAssertionsLink(record)}${buildAssertionUrlSearch({ type: AssertionType.Sql })}`}
                            onClick={() => {
                                analytics.event({
                                    type: EventType.DatasetHealthClickEvent,
                                    tabType: 'AssertionsByAsset',
                                    target: 'asset_assertions',
                                    subTarget: 'sql_results',
                                    targetUrn: record.urn,
                                });
                            }}
                        >
                            <HealthStatusIndicator status={sqlHealth?.status} />
                        </HealthStatusIndicatorLink>
                    </Tooltip>
                );
            },
        },
        // Do not show custom assertions column if there are no custom assertions
        ...(hasCustomAssertions
            ? [
                  {
                      key: 'custom',
                      title: 'Custom',
                      width: '80px',
                      render: (record) => {
                          const assertionHealth = getAssertionHealth(record.health);
                          const customHealth = getHealthForAssertionType(AssertionType.Custom, assertionHealth);
                          const message = customHealth
                              ? `${customHealth.statusCount} ${customHealth.statusCount === 1 ? 'is' : 'are'} ${HealthStatusToText[customHealth.status]} (${customHealth.total} total assertions)`
                              : // TODO: link to custom assertions reporting documentation
                                'No custom assertions have run yet. Create some with the SDK to see results here.';
                          return (
                              <Tooltip title={message}>
                                  <HealthStatusIndicatorLink
                                      to={`${getAssertionsLink(record)}${buildAssertionUrlSearch({ type: AssertionType.Custom })}`}
                                      onClick={() => {
                                          analytics.event({
                                              type: EventType.DatasetHealthClickEvent,
                                              tabType: 'AssertionsByAsset',
                                              target: 'asset_assertions',
                                              subTarget: 'custom_results',
                                              targetUrn: record.urn,
                                          });
                                      }}
                                  >
                                      <HealthStatusIndicator status={customHealth?.status} />
                                  </HealthStatusIndicatorLink>
                              </Tooltip>
                          );
                      },
                  },
              ]
            : []),
        // Do not show external assertions column if there are no external assertions
        ...(hasExternalAssertions
            ? [
                  {
                      key: 'dataset',
                      title: 'External',
                      width: '80px',
                      render: (record) => {
                          const assertionHealth = getAssertionHealth(record.health);
                          const datasetHealth = getHealthForAssertionType(AssertionType.Dataset, assertionHealth);
                          const message = datasetHealth
                              ? `${datasetHealth.statusCount} ${datasetHealth.statusCount === 1 ? 'is' : 'are'} ${HealthStatusToText[datasetHealth.status]} (${datasetHealth.total} total assertions)`
                              : // TODO: Add a link to the external data quality tool documentation
                                'No external assertions have been reported yet. Integrate an external data quality tool to see results here.';
                          return (
                              <Tooltip title={message}>
                                  <HealthStatusIndicatorLink
                                      to={`${getAssertionsLink(record)}${buildAssertionUrlSearch({ type: AssertionType.Dataset })}`}
                                      onClick={() => {
                                          analytics.event({
                                              type: EventType.DatasetHealthClickEvent,
                                              tabType: 'AssertionsByAsset',
                                              target: 'asset_assertions',
                                              subTarget: 'external_results',
                                              targetUrn: record.urn,
                                          });
                                      }}
                                  >
                                      <HealthStatusIndicator status={datasetHealth?.status} />
                                  </HealthStatusIndicatorLink>
                              </Tooltip>
                          );
                      },
                  },
              ]
            : []),

        /* ************************* Last Run Column ************************* */
        {
            key: 'lastAssertionResultAt',
            title: 'Last Activity',
            render: (record) => {
                const lastAssertionResultAt = getAssertionHealth(record.health)?.reportedAt;
                const formattedLabel = lastAssertionResultAt ? getTimeFromNow(lastAssertionResultAt) : 'Unknown';
                return (
                    <Tooltip
                        title={
                            lastAssertionResultAt
                                ? `The last assertion run was on ${formatTimestamp(lastAssertionResultAt, 'MMM d, YYYY h:mm a')}. Open to see more.`
                                : 'Unable to determine when an assertion was last run on this asset.'
                        }
                        placement="topLeft"
                    >
                        <Text style={{ display: 'inline' }}>{formattedLabel}</Text>
                    </Tooltip>
                );
            },
        },
    ];

    /* ------------************************* Render Table *************************------------ */
    return (
        <Container>
            <Table columns={columns} data={datasets} isLoading={isLoading} isScrollable maxHeight="100%" />
            <Pagination
                currentPage={page}
                total={total}
                itemsPerPage={pageSize}
                onPageChange={(newPage) => setPage(newPage)}
                loading={isLoading}
            />
        </Container>
    );
};
