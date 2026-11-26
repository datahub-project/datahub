import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router';

import { getSiblingWithUrn } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { IncidentDetailDrawer } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentDetailDrawer';
import { IncidentAction } from '@app/entityV2/shared/tabs/Incident/constant';
import { useIncidentsTableColumns, useOpenIncidentDetailModal } from '@app/entityV2/shared/tabs/Incident/hooks';
import { StyledTableContainer } from '@app/entityV2/shared/tabs/Incident/styledComponents';
import {
    IncidentGroup,
    IncidentListFilter,
    IncidentTable,
    IncidentTableRow,
} from '@app/entityV2/shared/tabs/Incident/types';
import { IncidentPriorityIcon } from '@app/sharedV2/incidentPriorityLabel';
import { IncidentStageIcon } from '@app/sharedV2/incidentStagePill';
import { getIncidentStageLabel } from '@app/sharedV2/incidentStagePill/constant';
import { getCapitalizeWord } from '@app/sharedV2/textUtil';
import { Table } from '@src/alchemy-components';
import { TableGroupByProps } from '@src/alchemy-components/components/Table/types';
import { useEntityData } from '@src/app/entityV2/shared/EntityContext';
import { EntityPrivileges } from '@src/types.generated';

type Props = {
    incidentData: IncidentTable;
    filter: IncidentListFilter;
    refetch: () => void;
    privileges?: EntityPrivileges;
};

const EMPTY_GROUPS: IncidentGroup[] = [];
const EMPTY_KEYS: string[] = [];

export const IncidentListTable = ({ incidentData, filter, refetch, privileges }: Props) => {
    const { t } = useTranslation('entity.profile.incident');
    const { urn, entityData } = useEntityData();
    const { groupBy } = filter;
    const groups: IncidentGroup[] = (groupBy && incidentData?.groupBy?.[groupBy]) || EMPTY_GROUPS;

    const location = useLocation();
    const linkedIncidentUrn = new URLSearchParams(location.search).get('incident_urn');

    // Groups the user closed, kept per grouping so switching "Group by" opens the new grouping fully
    // instead of carrying over open/closed state from group names that no longer exist.
    const [collapsedByGrouping, setCollapsedByGrouping] = useState<Record<string, string[]>>({});
    const collapsedKeys = (groupBy && collapsedByGrouping[groupBy]) || EMPTY_KEYS;

    // A link to one incident opens only that incident's group. It applies once per navigation and grouping,
    // so following a link (even the same one again) or switching grouping opens the linked group, while the
    // user's own toggles in between are left alone.
    const appliedLinksRef = useRef<Set<string>>(new Set());
    useEffect(() => {
        if (!groupBy || !linkedIncidentUrn || !groups.length) return;
        const linkKey = `${groupBy}:${linkedIncidentUrn}:${location.key}`;
        if (appliedLinksRef.current.has(linkKey)) return;
        const linkedGroup = groups.find((group) =>
            group.incidents.some((incident) => incident.urn === linkedIncidentUrn),
        );
        if (!linkedGroup) return;
        appliedLinksRef.current.add(linkKey);
        setCollapsedByGrouping((previous) => ({
            ...previous,
            [groupBy]: groups.map((group) => group.name).filter((name) => name !== linkedGroup.name),
        }));
    }, [groupBy, linkedIncidentUrn, location.key, groups]);

    // get columns data from the custom hooks
    const incidentsTableCols = useIncidentsTableColumns(refetch, privileges);

    const [focusIncidentUrn, setFocusIncidentUrn] = useState<string | null>(null);

    const focusedIncident = incidentData.incidents.find((incident) => incident.urn === focusIncidentUrn);
    const focusedEntityUrn = focusedIncident ? entityData?.urn : undefined;
    const focusedIncidentEntity =
        focusedEntityUrn && entityData ? getSiblingWithUrn(entityData, focusedEntityUrn) : undefined;

    useOpenIncidentDetailModal(setFocusIncidentUrn);

    useEffect(() => {
        if (focusIncidentUrn && !focusedIncident) {
            setFocusIncidentUrn(null);
        }
    }, [focusIncidentUrn, focusedIncident]);

    // Grouped rows list each group's incidents newest first until a column sort is picked.
    const groupedRows = useMemo(
        () => groups.flatMap((group) => group.incidents.slice().sort((a, b) => b.created - a.created)),
        [groups],
    );

    const tableGroupBy = useMemo((): TableGroupByProps<IncidentTableRow> | undefined => {
        if (!groupBy) return undefined;
        const groupNames = groups.map((group) => group.name);
        const groupNameByIncidentUrn = new Map<string, string>(
            groups.flatMap((group) => group.incidents.map((incident): [string, string] => [incident.urn, group.name])),
        );
        return {
            getGroupKey: (incident) => groupNameByIncidentUrn.get(incident.urn) ?? '',
            groupOrder: (a, b) => groupNames.indexOf(a.key) - groupNames.indexOf(b.key),
            renderGroupHeader: ({ key }) => {
                // Priority and stage have icons; category and state groups are label-only.
                if (groupBy === 'priority') {
                    return (
                        <>
                            <IncidentPriorityIcon priority={key} />
                            {getCapitalizeWord(key)}
                        </>
                    );
                }
                if (groupBy === 'stage') {
                    return (
                        <>
                            <IncidentStageIcon stage={key} />
                            {getIncidentStageLabel(t)[key] ?? getCapitalizeWord(key)}
                        </>
                    );
                }
                return getCapitalizeWord(key);
            },
            collapsedKeys,
            onCollapsedChange: (keys) => setCollapsedByGrouping((previous) => ({ ...previous, [groupBy]: keys })),
            groupHeaderDataTestId: ({ key }) => `incident-group-${key}`,
        };
    }, [groupBy, groups, collapsedKeys, t]);

    const rowClassName = (record: IncidentTableRow) =>
        record.urn === focusIncidentUrn ? 'acryl-selected-table-row' : 'acryl-incident-table-row';

    return (
        <>
            <StyledTableContainer style={{ height: '100vh', overflow: 'hidden' }}>
                <Table<IncidentTableRow>
                    columns={incidentsTableCols}
                    data={groupBy ? groupedRows : incidentData.incidents || []}
                    rowKey="urn"
                    showHeader
                    isScrollable
                    groupBy={tableGroupBy}
                    onRowClick={(record) => setFocusIncidentUrn(record.urn)}
                    rowClassName={rowClassName}
                    rowDataTestId={(record) => `incident-row-${record.title}`}
                />
            </StyledTableContainer>
            {focusIncidentUrn && focusedIncidentEntity && (
                <IncidentDetailDrawer
                    entity={{
                        urn,
                    }}
                    mode={IncidentAction.EDIT}
                    incident={focusedIncident}
                    privileges={privileges}
                    onCancel={() => setFocusIncidentUrn(null)}
                    onSubmit={() => {
                        refetch();
                    }}
                />
            )}
        </>
    );
};
