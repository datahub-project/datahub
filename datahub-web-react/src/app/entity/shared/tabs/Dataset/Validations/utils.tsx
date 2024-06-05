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
    CodeOutlined,
    ExclamationCircleOutlined,
} from '@ant-design/icons';
import {
    Assertion,
    AssertionResultType,
    AssertionType,
    // CronSchedule,
    // DatasetFreshnessSourceType,
    // DatasetVolumeSourceType,
    EntityType,
    // Monitor,
    // MonitorMode,
} from '../../../../../../types.generated';
import { sortAssertions } from './assertionUtils';
import { AssertionGroup, AssertionStatusSummary } from './types';
import { lowerFirstLetter } from '../../../../../shared/textUtil';
// import { useIngestionSourceForEntityQuery } from '../../../../../../graphql/ingestion.generated';
// import {
//     GetDatasetAssertionsWithMonitorsQuery,
//     MonitorDetailsFragment,
// } from '../../../../../../graphql/monitor.generated';

export const SUCCESS_COLOR_HEX = '#52C41A';
export const FAILURE_COLOR_HEX = '#F5222D';
export const WARNING_COLOR_HEX = '#FA8C16';

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

const StyledCloseOutlined = styled(CloseOutlined)`
    && {
        color: ${FAILURE_COLOR_HEX};
        font-size: 14px;
        padding: 0px;
        margin: 0px;
    }
`;

const StyledExclamationOutlined = styled(ExclamationCircleOutlined)`
    && {
        color: ${WARNING_COLOR_HEX};
        font-size: 14px;
        padding: 0px;
        margin: 0px;
    }
`;

const StyledCodeOutlined = styled(CodeOutlined)`
    && {
        margin: 0px;
        padding: 0px;
        margin-right: 8px;
        font-size: 18px;
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
        requiresConnectionSupportedByMonitors: false,
    },
    {
        name: 'Schema',
        description: "Define & monitor your expectations about the table's columns and their types",
        icon: <StyledCodeOutlined />,
        type: AssertionType.DataSchema,
        entityTypes: [EntityType.Dataset],
        enabled: true,
        visible: true,
    },
    {
        name: 'Custom',
        description: 'Define & monitor your expectations using custom SQL rules',
        icon: <StyledConsoleSqlOutlined />,
        type: AssertionType.Sql,
        entityTypes: [EntityType.Dataset],
        enabled: true,
        visible: true,
        requiresConnectionSupportedByMonitors: true,
    },
    {
        name: 'Other',
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

export type AssertionWithMonitorDetails = Assertion & {
    monitors?: any[]; // should almost always have 0-1 items
};

export const tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery = (
    queryData?: any, //GetDatasetAssertionsWithMonitorsQuery,
): AssertionWithMonitorDetails[] | undefined => {
    return queryData?.dataset?.assertions?.assertions?.map((assertion) => ({
        ...(assertion as Assertion),
        monitors:
            assertion.monitor?.relationships?.filter((r) => r.entity?.__typename === 'Monitor').map((r) => r.entity) ??
            [],
        // .map((r) => r.entity as MonitorDetailsFragment) ?? [],
    }));
};

/**
 * Returns a status summary for the assertions associated with a Dataset.
 *
 * @param assertions The assertions to extract the summary for
 */
export const getAssertionsSummary = (assertions: AssertionWithMonitorDetails[]): AssertionStatusSummary => {
    const summary = {
        passing: 0,
        failing: 0,
        erroring: 0,
        total: 0,
        totalAssertions: assertions.length,
    };
    assertions.forEach((assertion) => {
        // Skip inactive monitors
        // NOTE: we don't assert that the status is Active, because in cases of external assertions they won't have monitors
        const maybeInactiveMonitor = assertion.monitors?.find(
            (item) => item.info?.status.mode === 'IN_ACTIVE', //MonitorMode.Inactive,
        );
        if (maybeInactiveMonitor) {
            return;
        }

        if ((assertion.runEvents?.runEvents?.length || 0) > 0) {
            const mostRecentRun = assertion.runEvents?.runEvents?.[0];
            const resultType = mostRecentRun?.result?.type;
            if (AssertionResultType.Success === resultType) {
                summary.passing++;
            }
            if (AssertionResultType.Failure === resultType) {
                summary.failing++;
            }
            // if (AssertionResultType.Error === resultType) {
            // if ('ERROR' === resultType) {
            //     summary.erroring++;
            // }
            // if ('INIT' !== resultType) {
            //     summary.total++; // only count assertions for which there is one completed run event, ignoring INIT statuses!
            // }
        }
    });
    return summary;
};

/**
 * TODO: We will remove this mapping code once we replace the OSS legacy assertions summary with the new
 * format.
 */
export const getLegacyAssertionsSummary = (assertions: Assertion[]) => {
    const newSummary = getAssertionsSummary(assertions);
    return {
        failedRuns: newSummary.failing,
        succeededRuns: newSummary.passing,
        erroredRuns: newSummary.erroring,
        totalRuns: newSummary.total,
        totalAssertions: newSummary.totalAssertions,
    };
};

// /**
//  * Returns a list of assertion groups, where assertions are grouped
//  * by their "type" or "category". Each group includes the assertions inside, along with
//  * a summary of passing and failing assertions for the group.
//  *
//  * @param assertions The assertions to group
//  */
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
            summary: getAssertionsSummary(groupedAssertions),
            type,
        };
        assertionGroups.push(newGroup);
    });

    return assertionGroups;
};

// TODO: Make this the default inside DatasetAssertionsSummary.tsx.
export const getAssertionGroupSummaryIcon = (summary: AssertionStatusSummary) => {
    if (summary.total === 0) {
        return null;
    }
    if (summary.passing === summary.total) {
        return <StyledCheckOutlined />;
    }
    if (summary.erroring > 0) {
        return <StyledExclamationOutlined />;
    }
    return <StyledCloseOutlined />;
};

// TODO: Make this the default inside DatasetAssertionsSummary.tsx.
export const getAssertionGroupSummaryMessage = (summary: AssertionStatusSummary) => {
    if (summary.total === 0) {
        return 'No assertions have run';
    }
    if (summary.passing === summary.total) {
        return 'All assertions are passing';
    }
    if (summary.erroring > 0) {
        return 'An error is preventing some assertions from running';
    }
    if (summary.failing === summary.total) {
        return 'All assertions are failing';
    }
    return 'Some assertions are failing';
};

/**
 * Returns the next scheduled run of a cron schedule, in the local timezone of teh user.
 *
 * @param schedule a cron schedule
 */
// export const getNextScheduleEvaluationTimeMs = (schedule: CronSchedule) => {
//     try {
//         const interval = cronParser.parseExpression(schedule.cron, { tz: schedule.timezone });
//         const nextDate = interval.next().toDate(); // Get next date as JavaScript Date object
//         const userTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
//         const nextDateInUserTz = moment.tz(nextDate, userTimezone); // Convert to user's timezone
//         return nextDateInUserTz.valueOf();
//     } catch (e) {
//         return undefined;
//     }
// };

export const getAssertionTypesForEntityType = (entityType: EntityType, monitorsConnectionForEntityExists: boolean) => {
    return ASSERTION_INFO.filter((type) => type.entityTypes.includes(entityType)).map((type) => ({
        ...type,
        enabled: type.enabled && (!type.requiresConnectionSupportedByMonitors || monitorsConnectionForEntityExists),
    }));
};

export const isMonitorActive = (monitor: any) => {
    return monitor.info?.status?.mode === 'active';
};

export const getCronAsText = (interval: string, options: { verbose: boolean } = { verbose: false }) => {
    const { verbose } = options;
    if (interval) {
        try {
            return {
                text: `${lowerFirstLetter(cronstrue.toString(interval, { verbose }))}.`,
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
        assertionParameters?.datasetFreshnessParameters?.sourceType === 'DATAHUB_OPERATION' || //DatasetFreshnessSourceType.DatahubOperation ||
        assertionParameters?.datasetVolumeParameters?.sourceType === 'DATAHUB_DATASET_PROFILE' //DatasetVolumeSourceType.DatahubDatasetProfile
    );
};

// export const getEntityUrnForAssertion = (assertion: Assertion) => {
export const getEntityUrnForAssertion = (assertion: any) => {
    if (assertion.info?.type === AssertionType.Dataset) {
        return assertion.info?.datasetAssertion?.datasetUrn;
    }
    if (assertion.info?.type === AssertionType.Freshness) {
        return assertion.info?.freshnessAssertion?.entityUrn;
    }
    if (assertion.info?.type === AssertionType.Volume) {
        return assertion.info?.volumeAssertion?.entityUrn;
    }
    if (assertion.info?.type === AssertionType.Field) {
        return assertion.info?.fieldAssertion?.entityUrn;
    }
    if (assertion.info?.type === AssertionType.Sql) {
        return assertion.info?.sqlAssertion?.entityUrn;
    }
    if (assertion.info?.type === AssertionType.DataSchema) {
        return assertion.info?.schemaAssertion?.entityUrn;
    }
    console.error(`Unable to extract entity urn from unrecognized assertion with type ${assertion.info?.type}`);
    return undefined;
};

// export const useConnectionForEntityExists = (entityUrn: string) => {
//     const { data: ingestionSourceData } = useIngestionSourceForEntityQuery({
//         variables: { urn: entityUrn as string },
//         fetchPolicy: 'cache-first',
//     });

//     return !!ingestionSourceData?.ingestionSourceForEntity?.urn;
// };

/**
 * Checks if a connection exists for an entity that is able to run test assertion queries
 * @param entityUrn
 * @returns {boolean} optimistically returns true
 */
// export const useConnectionWithTestAssertionCapabilitiesForEntityExists = (entityUrn: string): boolean => {
//     const { data: ingestionSourceData } = useIngestionSourceForEntityQuery({
//         variables: { urn: entityUrn as string },
//         fetchPolicy: 'cache-first',
//     });

//     // Only embedded executors can run tests right now
//     // If executorId is null, we'll assume it is an embedded executor.
//     // If the executorId starts with 'default', we assume it's an embedded executor
//     // See setup docs: https://www.notion.so/acryldata/How-to-configure-Remote-Executor-e9ed044b438d4789afcd530952d73944?pvs=4#14237a6d6dd04fcfb2abd45f16c6d63c
//     // and design docs:  https://www.notion.so/acryldata/Remote-Executor-V2-Design-593d41280c4a4e34805def00b3f47a65?pvs=4#fe2a4481fbe74f379eb35cd10546b3b8
//     const maybeExecutorId = ingestionSourceData?.ingestionSourceForEntity?.config?.executorId;
//     return !maybeExecutorId || maybeExecutorId.toLowerCase().startsWith('default');
// };
