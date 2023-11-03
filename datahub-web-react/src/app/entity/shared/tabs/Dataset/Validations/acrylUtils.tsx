import React from 'react';
import styled from 'styled-components';
import * as moment from 'moment-timezone';
import * as cronParser from 'cron-parser';
import cronstrue from 'cronstrue';
import {
    ClockCircleOutlined,
    TableOutlined,
    ProjectOutlined,
    ConsoleSqlOutlined,
    CheckOutlined,
    CloseOutlined,
    ApiOutlined,
} from '@ant-design/icons';
import {
    Assertion,
    AssertionResultType,
    AssertionType,
    CronSchedule,
    DatasetFreshnessSourceType,
    DatasetVolumeSourceType,
    EntityType,
    Monitor,
    MonitorMode,
} from '../../../../../../types.generated';
import { sortAssertions } from './assertionUtils';
import { AssertionGroup, AssertionGroupSummary } from './acrylTypes';
import { lowerFirstLetter } from '../../../../../shared/textUtil';
import { useIngestionSourceForEntityQuery } from '../../../../../../graphql/ingestion.generated';

const SUCCESS_COLOR_HEX = '#52C41A';
const FAILURE_COLOR_HEX = '#F5222D';

const StyledClockCircleOutlined = styled(ClockCircleOutlined)`
    && {
        margin: 0px;
        padding: 0px;
        margin-right: 8px;
        font-size: 14px;
    }
`;

const StyledTableOutlined = styled(TableOutlined)`
    && {
        margin: 0px;
        padding: 0px;
        margin-right: 8px;
        font-size: 18px;
    }
`;

const StyledProjectOutlined = styled(ProjectOutlined)`
    && {
        margin: 0px;
        padding: 0px;
        margin-right: 8px;
        font-size: 18px;
    }
`;

const StyledConsoleSqlOutlined = styled(ConsoleSqlOutlined)`
    && {
        margin: 0px;
        padding: 0px;
        margin-right: 8px;
        font-size: 18px;
    }
`;

const StyledApiOutlined = styled(ApiOutlined)`
    && {
        margin: 0px;
        padding: 0px;
        margin-right: 8px;
        font-size: 18px;
    }
`;

const StyledCheckOutlined = styled(CheckOutlined)`
    && {
        color: ${SUCCESS_COLOR_HEX};
        font-size: 14px;
        padding: 0px;
        margin: 0px;
    }
`;

const StyleCloseOutlined = styled(CloseOutlined)`
    && {
        color: ${FAILURE_COLOR_HEX};
        font-size: 14px;
        padding: 0px;
        margin: 0px;
    }
`;

export const ASSERTION_INFO = [
    {
        name: 'Freshness',
        description: 'Define & monitor your expectations about when this dataset should be updated',
        icon: <StyledClockCircleOutlined />,
        type: AssertionType.Freshness,
        entityTypes: [EntityType.Dataset],
        enabled: true,
        visible: true,
    },
    {
        name: 'Volume',
        description: 'Define & monitor your expectations about the size of this dataset',
        icon: <StyledTableOutlined />,
        type: AssertionType.Volume,
        entityTypes: [EntityType.Dataset],
        enabled: true,
        visible: true,
    },
    {
        name: 'Column',
        description: 'Define & monitor your expectations about the values in a column',
        icon: <StyledProjectOutlined />,
        type: AssertionType.Field,
        entityTypes: [EntityType.Dataset],
        enabled: true,
        visible: true,
        requiresConnection: false,
    },
    {
        name: 'Custom',
        description: 'Define & monitor your expectations using custom SQL rules',
        icon: <StyledConsoleSqlOutlined />,
        type: AssertionType.Sql,
        entityTypes: [EntityType.Dataset],
        enabled: true,
        visible: true,
        requiresConnection: true,
    },
    {
        name: 'External',
        description: 'Assertions that are defined and maintained outside of DataHub.',
        icon: <StyledApiOutlined />,
        type: AssertionType.Dataset,
        entityTypes: [EntityType.Dataset],
        enabled: false,
        visible: false,
    },
];

const ASSERTION_TYPE_TO_INFO = new Map();
ASSERTION_INFO.forEach((info) => {
    ASSERTION_TYPE_TO_INFO.set(info.type, info);
});

const getAssertionGroupName = (type: AssertionType): string => {
    return ASSERTION_TYPE_TO_INFO.has(type) ? ASSERTION_TYPE_TO_INFO.get(type).name : 'Unknown';
};

const getAssertionGroupTypeIcon = (type: AssertionType) => {
    return ASSERTION_TYPE_TO_INFO.has(type) ? ASSERTION_TYPE_TO_INFO.get(type).icon : undefined;
};

/**
 * Returns a status summary for the assertions associated with a Dataset.
 *
 * @param assertions The assertions to extract the summary for
 */
export const getAssertionGroupSummary = (assertions: Assertion[]) => {
    const summary = {
        failedRuns: 0,
        succeededRuns: 0,
        totalRuns: 0,
        erroredRuns: 0,
        totalAssertions: assertions.length,
    };
    assertions.forEach((assertion) => {
        if ((assertion.runEvents?.runEvents?.length || 0) > 0) {
            const mostRecentRun = assertion.runEvents?.runEvents?.[0];
            const resultType = mostRecentRun?.result?.type;
            if (AssertionResultType.Success === resultType) {
                summary.succeededRuns++;
            }
            if (AssertionResultType.Failure === resultType) {
                summary.failedRuns++;
            }
            if (AssertionResultType.Error === resultType) {
                summary.erroredRuns++;
            }
            if (AssertionResultType.Init !== resultType) {
                summary.totalRuns++; // only count assertions for which there is one completed run event, ignoring INIT statuses!
            }
        }
    });
    return summary;
};

/**
 * Returns a list of assertion groups, where assertions are grouped
 * by their "type" or "category". Each group includes the assertions inside, along with
 * a summary of passing and failing assertions for the group.
 *
 * @param assertions The assertions to group
 */
export const createAssertionGroups = (assertions: Array<Assertion>): AssertionGroup[] => {
    // Pre-sort the list of assertions based on which has been most recently executed.
    assertions.sort(sortAssertions);

    const typeToAssertions = new Map();
    assertions
        .filter((assertion) => assertion.info?.type)
        .forEach((assertion) => {
            const groupType = assertion.info?.type;
            const groupedAssertions = typeToAssertions.get(groupType) || [];
            groupedAssertions.push(assertion);
            typeToAssertions.set(groupType, groupedAssertions);
        });

    // Now, create summary for each type and build the AssertionGroup object
    const assertionGroups: AssertionGroup[] = [];
    typeToAssertions.forEach((groupedAssertions, type) => {
        const newGroup: AssertionGroup = {
            name: getAssertionGroupName(type),
            icon: getAssertionGroupTypeIcon(type),
            assertions: groupedAssertions,
            summary: getAssertionGroupSummary(groupedAssertions),
        };
        assertionGroups.push(newGroup);
    });

    return assertionGroups;
};

export const getAssertionGroupSummaryIcon = (summary: AssertionGroupSummary) => {
    if (summary.succeededRuns === 0 && summary.failedRuns === 0) {
        return null;
    }
    if (summary.succeededRuns === summary.totalRuns) {
        return <StyledCheckOutlined />;
    }
    return <StyleCloseOutlined />;
};

export const getAssertionGroupSummaryMessage = (summary: AssertionGroupSummary) => {
    if (summary.totalRuns === 0) {
        return 'No assertions have run';
    }
    if (summary.succeededRuns === summary.totalRuns) {
        return 'All assertions are passing';
    }
    if (summary.erroredRuns > 0) {
        return 'An error is preventing some assertions from running';
    }
    if (summary.failedRuns === summary.totalRuns) {
        return 'All assertions are failing';
    }
    return 'Some assertions are failing';
};

/**
 * Returns the next scheduled run of a cron schedule, in the local timezone of teh user.
 *
 * @param schedule a cron schedule
 */
export const getNextScheduleEvaluationTimeMs = (schedule: CronSchedule) => {
    try {
        const interval = cronParser.parseExpression(schedule.cron, { tz: schedule.timezone });
        const nextDate = interval.next().toDate(); // Get next date as JavaScript Date object
        const userTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
        const nextDateInUserTz = moment.tz(nextDate, userTimezone); // Convert to user's timezone
        return nextDateInUserTz.valueOf();
    } catch (e) {
        return undefined;
    }
};

export const getAssertionTypesForEntityType = (entityType: EntityType, connectionForEntityExists: boolean) => {
    return ASSERTION_INFO.filter((type) => type.entityTypes.includes(entityType)).map((type) => ({
        ...type,
        enabled: type.enabled && (!type.requiresConnection || connectionForEntityExists),
    }));
};

export const isMonitorActive = (monitor: Monitor) => {
    return monitor.info?.status?.mode === MonitorMode.Active;
};

export const getCronAsText = (interval: string) => {
    if (interval) {
        try {
            return {
                text: `${lowerFirstLetter(cronstrue.toString(interval, { verbose: false }))}.`,
                error: false,
            };
        } catch (e) {
            return {
                text: undefined,
                error: true,
            };
        }
    }
    return {
        text: undefined,
        error: false,
    };
};

export const canManageAssertionMonitor = (monitor: any, connectionForEntityExists: boolean) => {
    if (connectionForEntityExists) return true;

    const assertionParameters = monitor?.info?.assertionMonitor?.assertions?.[0]?.parameters;
    return (
        assertionParameters?.datasetFreshnessParameters?.sourceType === DatasetFreshnessSourceType.DatahubOperation ||
        assertionParameters?.datasetVolumeParameters?.sourceType === DatasetVolumeSourceType.DatahubDatasetProfile
    );
};

export const useConnectionForEntityExists = (entityUrn: string) => {
    const { data: ingestionSourceData } = useIngestionSourceForEntityQuery({
        variables: { urn: entityUrn as string },
        fetchPolicy: 'cache-first',
    });

    return !!ingestionSourceData?.ingestionSourceForEntity?.urn;
};
