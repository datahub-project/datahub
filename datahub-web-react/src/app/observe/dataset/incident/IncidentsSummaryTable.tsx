import { Column, Pagination, Table, Text } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { buildIncidentUrlSearch } from '@app/entityV2/shared/tabs/Incident/utils';
import { renderOwners } from '@app/observe/dataset/shared/shared';
import { healthUrlSuffix } from '@app/previewV2/HealthPopover';
import { getTimeFromNow } from '@app/shared/time/timeUtils';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ActiveIncidentHealthDetails, Dataset, EntityType, Health, HealthStatusType, Maybe } from '@types';

export const DatasetNameColumn = styled(Link)`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const Container = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: hidden;
    height: 100%;
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
                return (
                    <DatasetNameColumn to={getIncidentsLink(record)} data-testid={`preview-${record.urn}`}>
                        <PlatformIcon platform={record.platform} />
                        <Text weight="semiBold">{record.name}</Text>
                    </DatasetNameColumn>
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
                    <Link to={getIncidentsLink(record)}>
                        <Text>{count} active</Text>
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
