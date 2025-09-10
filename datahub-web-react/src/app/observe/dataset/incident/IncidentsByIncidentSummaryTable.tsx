import { Column, Table, Text, Tooltip, colors } from '@components';
import { Check, Warning } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import {
    INCIDENTS_DOCS_LINK,
    IncidentWithRelationships,
} from '@app/observe/dataset/incident/IncidentsByIncidentSummary.utils';
import { INCIDENT_TYPE_OPTIONS } from '@app/observe/dataset/incident/constants';
import {
    IncidentAssetsColumn,
    IncidentAssigneesColumn,
    IncidentTimestampTooltip,
    IncidentTitleTooltip,
} from '@app/observe/dataset/incident/tableComponents';
import { Pagination } from '@src/alchemy-components';
import { IncidentPriorityLabel } from '@src/alchemy-components/components/IncidentPriorityLabel/IncidentPriorityLabel';
import { IncidentStagePill } from '@src/alchemy-components/components/IncidentStagePill/IncidentStagePill';
import EmptyFormsImage from '@src/images/empty-forms.svg?react';

import { IncidentState, IncidentType } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: hidden;
`;

const EmptyState = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 90%;
`;

const StyledEmptyFormsImage = styled(EmptyFormsImage)`
    margin-bottom: 16px;
`;

const getIncidentTypeLabel = (incidentType: IncidentType) => {
    const typeOption = INCIDENT_TYPE_OPTIONS.find((option) => option.value === incidentType);
    return typeOption?.name || 'Unknown';
};

type Props = {
    incidents: IncidentWithRelationships[];
    total: number;
    page: number;
    setPage: (page: number) => void;
    pageSize: number;
    setPageSize: (size: number) => void;
    loading: boolean;
    hasModifiedDefaultFilters: boolean;
};

export const IncidentsByIncidentSummaryTable = ({
    incidents,
    total,
    page,
    setPage,
    pageSize,
    setPageSize,
    loading,
    hasModifiedDefaultFilters,
}: Props) => {
    const columns: Column<IncidentWithRelationships>[] = [
        {
            key: 'title',
            title: 'Incident',
            width: '400px', // or whatever width you prefer
            render: (record) => <IncidentTitleTooltip record={record} />,
        },
        {
            key: 'type',
            title: 'Type',
            render: (record) => {
                const { incidentType, customType } = record;

                return <Text size="md">{customType || getIncidentTypeLabel(incidentType)}</Text>;
            },
        },
        {
            key: 'priority',
            title: 'Priority',
            render: (record) => {
                const { priority } = record;
                return priority === undefined || priority === null ? null : (
                    <IncidentPriorityLabel priority={priority} />
                );
            },
        },
        {
            key: 'stage',
            title: 'Stage',
            render: (record) => {
                const status = record.incidentStatus;
                return <IncidentStagePill stage={status?.stage || ''} showLabel />;
            },
        },
        {
            key: 'assets',
            title: 'Affected Assets',
            render: (record) => <IncidentAssetsColumn record={record} />,
        },
        {
            key: 'assignees',
            title: 'Assignees',
            render: (record) => <IncidentAssigneesColumn record={record} />,
        },
        {
            key: 'status',
            title: 'Status',
            render: (record) => {
                const status = record.incidentStatus;
                const isActive = status?.state === IncidentState.Active;

                return isActive ? (
                    <Tooltip title="Active">
                        <Warning size={20} color={colors.yellow[600]} />
                    </Tooltip>
                ) : (
                    <Tooltip title="Resolved">
                        <Check size={20} color={colors.green[500]} />
                    </Tooltip>
                );
            },
        },
        {
            key: 'lastActivity',
            title: 'Last Activity',
            sorter: true,
            render: (record) => <IncidentTimestampTooltip record={record} />,
        },
    ];

    if (total === 0 && !loading) {
        return (
            <Container>
                <EmptyState>
                    {!hasModifiedDefaultFilters
                        ? [
                              <StyledEmptyFormsImage />,
                              <Text size="lg" weight="semiBold" key="title" color="gray" colorLevel={600}>
                                  No incidents found
                              </Text>,
                              <Text size="lg" color="gray" key="description" colorLevel={1700}>
                                  Incidents help you track and resolve issues across your data landscape.
                              </Text>,
                              <a href={INCIDENTS_DOCS_LINK} target="_blank" rel="noreferrer" key="link">
                                  <Text size="lg" weight="semiBold" color="primary">
                                      Learn more
                                  </Text>
                              </a>,
                          ]
                        : [
                              <StyledEmptyFormsImage />,
                              <Text size="lg" weight="semiBold" key="no-results" color="gray" colorLevel={600}>
                                  No incidents match your current filters
                              </Text>,
                              <Text size="md" color="gray" key="try-different" colorLevel={1700}>
                                  Try changing your filters to see more results.
                              </Text>,
                          ]}
                </EmptyState>
            </Container>
        );
    }

    return (
        <Container>
            <Table columns={columns} data={incidents} isLoading={loading} isScrollable maxHeight="100%" />
            <Pagination
                currentPage={page}
                total={total}
                itemsPerPage={pageSize}
                showSizeChanger
                onPageChange={(newPage) => setPage(newPage)}
                loading={loading}
                onShowSizeChange={(_, newSize) => setPageSize(newSize)}
            />
        </Container>
    );
};
