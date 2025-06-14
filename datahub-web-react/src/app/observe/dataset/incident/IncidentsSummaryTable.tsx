import { Column, Icon, Pagination, Table, Text } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { buildIncidentUrlSearch } from '@app/entityV2/shared/tabs/Incident/utils';
import { renderOwners } from '@app/observe/dataset/shared/shared';
import ContextPath from '@app/previewV2/ContextPath';
import { healthUrlSuffix } from '@app/previewV2/HealthPopover';
import { getParentEntities } from '@app/searchV2/filters/utils';
import { getTimeFromNow } from '@app/shared/time/timeUtils';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ActiveIncidentHealthDetails, Dataset, EntityType, Health, HealthStatusType, Maybe } from '@types';

export const DatasetNameColumn = styled(Link)`
    display: flex;
    align-items: center;
    gap: 8px;
    &:hover {
        text-decoration: underline;
    }
    margin-left: 8px;
`;

const Container = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: hidden;
    height: 100%;
`;

const DatasetNameColumnWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const getIncidentHealth = (entityHealth?: Maybe<Health[]>): ActiveIncidentHealthDetails | undefined => {
    return entityHealth?.find((h) => h.type === HealthStatusType.Incidents)?.activeIncidentHealthDetails ?? undefined;
};

type Props = {
    datasets: Dataset[];
    isLoading: boolean;
    page: number;
    setPage: (page: number) => void;
    pageSize: number;
    total: number;
};
export const IncidentsSummaryTable = ({ datasets, isLoading, page, setPage, pageSize, total }: Props) => {
    const entityRegistry = useEntityRegistry();

    const getIncidentsLink = (record: Dataset) =>
        `${entityRegistry.getEntityUrl(EntityType.Dataset, record.urn)}${healthUrlSuffix({ type: HealthStatusType.Incidents })}`;

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
                                to={getIncidentsLink(record)}
                                data-testid={`preview-${record.urn}`}
                                onClick={() => {
                                    analytics.event({
                                        type: EventType.DatasetHealthClickEvent,
                                        tabType: 'IncidentsByAsset',
                                        target: 'asset_incidents',
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
                const owners = record.ownership?.owners;
                return renderOwners(owners ?? [], entityRegistry);
            },
        },

        /* ************************* Incident details ************************* */
        {
            key: 'count',
            title: 'Active Incidents',
            render: (record) => {
                const incidentHealth = getIncidentHealth(record.health);
                const count = incidentHealth?.count ?? 0;
                return (
                    <Link
                        style={{ display: 'flex', alignItems: 'center', gap: 4 }}
                        to={getIncidentsLink(record)}
                        onClick={() => {
                            analytics.event({
                                type: EventType.DatasetHealthClickEvent,
                                tabType: 'IncidentsByAsset',
                                target: 'asset_incidents',
                                targetUrn: record.urn,
                            });
                        }}
                    >
                        {count ? <Icon source="phosphor" icon="Warning" color="primary" size="sm" /> : null}
                        <Text color={count ? 'primary' : 'gray'}>
                            {count ? `${count} active` : 'No active incidents'}
                        </Text>
                    </Link>
                );
            },
        },

        {
            key: 'lastActivity',
            title: 'Recent Activity',
            render: (record) => {
                const incidentHealth = getIncidentHealth(record.health);
                const lastActivity = incidentHealth?.lastActivityAt;
                const lastActiveIncidentTitle = incidentHealth?.latestIncidentTitle;
                const lastActiveIncidentUrn = incidentHealth?.latestIncidentUrn;
                return (
                    <Link
                        to={
                            lastActiveIncidentUrn
                                ? buildIncidentUrlSearch({
                                      urn: lastActiveIncidentUrn,
                                      baseUrl: getIncidentsLink(record),
                                  })
                                : getIncidentsLink(record)
                        }
                        onClick={() => {
                            analytics.event({
                                type: EventType.DatasetHealthClickEvent,
                                tabType: 'IncidentsByAsset',
                                target: 'incident',
                                targetUrn: lastActiveIncidentUrn ?? '',
                            });
                        }}
                    >
                        <Text color="gray">
                            {lastActiveIncidentTitle ? (
                                <>
                                    &quot;<b>{lastActiveIncidentTitle}</b>&quot;
                                </>
                            ) : (
                                'Unknown incident'
                            )}
                        </Text>
                        <Text color="gray">
                            {lastActivity ? `Last updated ${getTimeFromNow(lastActivity)}` : 'Last activity unknown'}
                        </Text>
                    </Link>
                );
            },
        },
    ];

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
