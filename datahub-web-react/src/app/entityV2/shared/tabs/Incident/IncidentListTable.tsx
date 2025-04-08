import React, { useEffect, useState } from 'react';

import { useGetExpandedTableGroupsFromEntityUrnInUrl } from '@app/entityV2/shared/hooks';
import { getSiblingWithUrn } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { IncidentDetailDrawer } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentDetailDrawer';
import { IncidentAction } from '@app/entityV2/shared/tabs/Incident/constant';
import { useIncidentsTableColumns, useOpenIncidentDetailModal } from '@app/entityV2/shared/tabs/Incident/hooks';
import { StyledTableContainer } from '@app/entityV2/shared/tabs/Incident/styledComponents';
import { IncidentListFilter, IncidentTable } from '@app/entityV2/shared/tabs/Incident/types';
import { getSortedIncidents } from '@app/entityV2/shared/tabs/Incident/utils';
import { Table } from '@src/alchemy-components';
import { SortingState } from '@src/alchemy-components/components/Table/types';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { EntityPrivileges } from '@src/types.generated';

type Props = {
    incidentData: IncidentTable;
    filter: IncidentListFilter;
    refetch: () => void;
    privileges?: EntityPrivileges;
};

export const IncidentListTable = ({ incidentData, filter, refetch, privileges }: Props) => {
    const { entityData } = useEntityData();
    const { groupBy } = filter;

    const { expandedGroupIds, setExpandedGroupIds } = useGetExpandedTableGroupsFromEntityUrnInUrl(
        incidentData?.groupBy ? incidentData?.groupBy[groupBy] : [],
        { isGroupBy: !!groupBy },
        'incident_urn',
        (group) => group.incidents,
    );

    // get columns data from the custom hooks
    const incidentsTableCols = useIncidentsTableColumns(refetch, privileges);
    const [sortedOptions, setSortedOptions] = useState<{ sortColumn: string; sortOrder: SortingState }>({
        sortColumn: '',
        sortOrder: SortingState.ORIGINAL,
    });

    const [focusIncidentUrn, setFocusIncidentUrn] = useState<string | null>(null);

    const focusedIncident = incidentData.incidents.find((incident) => incident.urn === focusIncidentUrn);
    const focusedEntityUrn = focusedIncident ? entityData?.urn : undefined;
    const focusedIncidentEntity =
        focusedEntityUrn && entityData ? getSiblingWithUrn(entityData, focusedEntityUrn) : undefined;

    const getGroupData = () => {
        return (incidentData?.groupBy && incidentData?.groupBy[groupBy]) || [];
    };

    useOpenIncidentDetailModal(setFocusIncidentUrn);

    useEffect(() => {
        if (focusIncidentUrn && !focusedIncident) {
            setFocusIncidentUrn(null);
        }
    }, [focusIncidentUrn, focusedIncident]);

    const onIncidentExpand = (record) => {
        const key = record.name;
        setExpandedGroupIds((prev) => (prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]));
    };

    const rowClassName = (record) => {
        if (record.groupName) {
            return 'group-header';
        }
        if (record.urn === focusIncidentUrn) {
            return 'acryl-selected-table-row';
        }
        return 'acryl-incident-table-row';
    };

    const onRowClick = (record) => {
        setFocusIncidentUrn(record.urn);
    };

    const rowDataTestId = (record) => {
        return record.groupName ? `incident-group-${record.name}` : `incident-row-${record.title}`;
    };

    return (
        <>
            <StyledTableContainer style={{ height: '100vh', overflow: 'hidden' }}>
                <Table
                    columns={incidentsTableCols}
                    data={groupBy ? getGroupData() : incidentData.incidents || []}
                    showHeader
                    isScrollable
                    rowClassName={rowClassName}
                    handleSortColumnChange={({
                        sortColumn,
                        sortOrder,
                    }: {
                        sortColumn: string;
                        sortOrder: SortingState;
                    }) => setSortedOptions({ sortColumn, sortOrder })}
                    expandable={{
                        expandedRowRender: (record) => {
                            let sortedIncidents = record.incidents;
                            if (sortedOptions.sortColumn && sortedOptions.sortOrder) {
                                sortedIncidents = getSortedIncidents(record, sortedOptions);
                            }
                            return (
                                <Table
                                    columns={incidentsTableCols}
                                    data={sortedIncidents}
                                    showHeader={false}
                                    isBorderless
                                    isExpandedInnerTable
                                    onRowClick={onRowClick}
                                    rowClassName={rowClassName}
                                    rowDataTestId={rowDataTestId}
                                />
                            );
                        },
                        rowExpandable: () => !!groupBy,
                        expandIconPosition: 'end',
                        expandedGroupIds,
                    }}
                    onExpand={onIncidentExpand}
                    rowDataTestId={rowDataTestId}
                />
            </StyledTableContainer>
            {focusIncidentUrn && focusedIncidentEntity && (
                <IncidentDetailDrawer
                    urn={focusIncidentUrn}
                    mode={IncidentAction.EDIT}
                    incident={focusedIncident}
                    privileges={privileges}
                    onCancel={() => setFocusIncidentUrn(null)}
                    onSubmit={() => {
                        setTimeout(() => {
                            refetch();
                        }, 3000);
                    }}
                />
            )}
        </>
    );
};
