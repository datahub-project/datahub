import React from 'react';
import styled from 'styled-components';
import * as moment from 'moment-timezone';
import * as cronParser from 'cron-parser';
import cronstrue from 'cronstrue';
import { CheckOutlined, CloseOutlined, ApiOutlined, ExclamationCircleOutlined } from '@ant-design/icons';
import { ASSERTION_TYPE_TO_ICON_MAP } from '@src/app/entityV2/shared/tabs/Dataset/Validations/shared/constant';
import { GetDatasetAssertionsWithRunEventsQuery } from '@src/graphql/dataset.generated';
import {
    Assertion,
    AssertionResultType,
    AssertionType,
    CronSchedule,
    EntityType,
} from '../../../../../../types.generated';
import { sortAssertions } from './assertionUtils';
import { AssertionGroup, AssertionStatusSummary } from './acrylTypes';
import { lowerFirstLetter } from '../../../../../shared/textUtil';
import { GenericEntityProperties } from '../../../../../entity/shared/types';
import { toProperTitleCase } from '../../../utils';

export const SUCCESS_COLOR_HEX = '#52C41A';
export const FAILURE_COLOR_HEX = '#F5222D';
export const WARNING_COLOR_HEX = '#FA8C16';

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

const getStyledIconComponent = (type: AssertionType) => {
    const IconComponent = ASSERTION_TYPE_TO_ICON_MAP[type];

    // Wrap the JSX element in a styled div
    const StyledIcon = styled.div`
        margin: 0;
        padding: 0;
        margin-right: 8px;
        font-size: 18px;
        display: inline-flex;
        align-items: center;
    `;

    // Return the styled wrapper with the icon inside
    return () => <StyledIcon>{IconComponent}</StyledIcon>;
};

export const ASSERTION_INFO = [
    {
        name: 'Freshness',
        description: 'Define & monitor your expectations about when this dataset should be updated',
        icon: React.createElement(getStyledIconComponent(AssertionType.Freshness)),
        type: AssertionType.Freshness,
        entityTypes: [EntityType.Dataset],
        enabled: true,
        visible: true,
    },
    {
        name: 'Volume',
        description: 'Define & monitor your expectations about the size of this dataset',
        icon: React.createElement(getStyledIconComponent(AssertionType.Volume)),
        type: AssertionType.Volume,
        entityTypes: [EntityType.Dataset],
        enabled: true,
        visible: true,
    },
    {
        name: 'Column',
        description: 'Define & monitor your expectations about the values in a column',
        icon: React.createElement(getStyledIconComponent(AssertionType.Field)),
        type: AssertionType.Field,
        entityTypes: [EntityType.Dataset],
        enabled: true,
        visible: true,
        requiresConnectionSupportedByMonitors: false,
    },
    {
        name: 'Schema',
        description: "Define & monitor your expectations about the table's columns and their types",
        icon: React.createElement(getStyledIconComponent(AssertionType.DataSchema)),
        type: AssertionType.DataSchema,
        entityTypes: [EntityType.Dataset],
        enabled: true,
        visible: true,
    },
    {
        name: 'SQL',
        description: 'Define & monitor your expectations using custom SQL rules',
        icon: React.createElement(getStyledIconComponent(AssertionType.Sql)),
        type: AssertionType.Sql,
        entityTypes: [EntityType.Dataset],
        enabled: true,
        visible: true,
        requiresConnectionSupportedByMonitors: true,
    },
    {
        name: 'Other',
        description: 'Other assertions that are defined and maintained outside of DataHub.',
        icon: React.createElement(getStyledIconComponent(AssertionType.Dataset)),
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

export const getAssertionGroupName = (type: string): string => {
    return ASSERTION_TYPE_TO_INFO.has(type) ? ASSERTION_TYPE_TO_INFO.get(type).name : toProperTitleCase(type);
};

export const getAssertionGroupTypeIcon = (type: string) => {
    return ASSERTION_TYPE_TO_INFO.has(type) ? ASSERTION_TYPE_TO_INFO.get(type).icon : <StyledApiOutlined />;
};

export const tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery = (
    queryData?: GetDatasetAssertionsWithRunEventsQuery,
): Assertion[] | undefined => {
    return queryData?.dataset?.assertions?.assertions?.map((assertion) => ({
        ...(assertion as Assertion),
    }));
};

/**
 * Returns a status summary for the assertions associated with a Dataset.
 *
 * @param assertions The assertions to extract the summary for
 */
export const getAssertionsSummary = (assertions: Assertion[]): AssertionStatusSummary => {
    const summary = {
        passing: 0,
        failing: 0,
        erroring: 0,
        total: 0,
        totalAssertions: assertions.length,
    };
    assertions.forEach((assertion) => {
        if ((assertion.runEvents?.runEvents?.length || 0) > 0) {
            const mostRecentRun = assertion.runEvents?.runEvents?.[0];
            const resultType = mostRecentRun?.result?.type;
            if (AssertionResultType.Success === resultType) {
                summary.passing++;
            }
            if (AssertionResultType.Failure === resultType) {
                summary.failing++;
            }
            if (AssertionResultType.Error === resultType) {
                summary.erroring++;
            }
            if (AssertionResultType.Init !== resultType) {
                summary.total++; // only count assertions for which there is one completed run event, ignoring INIT statuses!
            }
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

export const getAssertionType = (assertion: Assertion): string | undefined => {
    return assertion?.info?.customAssertion?.type?.toUpperCase() || assertion?.info?.type?.toUpperCase();
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
            const assertionType: string | undefined = getAssertionType(assertion);
            const assertionGroup: string = assertionType || 'Unknown';
            const groupedAssertions = typeToAssertions.get(assertionGroup) || [];
            groupedAssertions.push(assertion);
            typeToAssertions.set(assertionGroup, groupedAssertions);
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
 * Returns the next scheduled run of a cron schedule, in the local timezone of the user.
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
        console.log('Failed to parse cron expression', e);
        return undefined;
    }
};

/**
 * Returns the previously scheduled run of a cron schedule, in the local timezone of the user.
 *
 * @param schedule a cron schedule
 * @param maybeFromDateTS
 */
export const getPreviousScheduleEvaluationTimeMs = (schedule: CronSchedule, maybeFromDateTS?: number) => {
    try {
        const interval = cronParser.parseExpression(schedule.cron, { tz: schedule.timezone });
        if (typeof maybeFromDateTS !== 'undefined') {
            interval.reset(maybeFromDateTS);
        }
        const prevDate = interval.prev().toDate(); // Get prev date as JavaScript Date object
        const userTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
        const prevDateInUserTz = moment.tz(prevDate, userTimezone); // Convert to user's timezone
        return prevDateInUserTz.valueOf();
    } catch (e) {
        console.log('Failed to parse cron expression', e);
        return undefined;
    }
};
export const getAssertionTypesForEntityType = (entityType: EntityType, monitorsConnectionForEntityExists: boolean) => {
    return ASSERTION_INFO.filter((type) => type.entityTypes.includes(entityType)).map((type) => ({
        ...type,
        enabled: type.enabled && (!type.requiresConnectionSupportedByMonitors || monitorsConnectionForEntityExists),
    }));
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

export const getEntityUrnForAssertion = (assertion: Assertion) => {
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
    if (assertion.info?.type === AssertionType.Custom) {
        return assertion.info?.customAssertion?.entityUrn;
    }
    console.error(`Unable to extract entity urn from unrecognized assertion with type ${assertion.info?.type}`);
    return undefined;
};

/**
 * Attempts to extract the sibling entity associated with a given urn.
 */
export const getSiblingWithUrn = (entityData: GenericEntityProperties, urn: string) => {
    if (entityData.urn === urn) {
        return entityData;
    }
    return entityData?.siblingsSearch?.searchResults
        ?.map((result) => result.entity)
        .find((entity) => entity?.urn === urn);
};

/**
 * Extract the siblings for an entity
 */
export const getSiblings = (entityData: GenericEntityProperties | null): GenericEntityProperties[] => {
    if (!entityData) return [];
    return entityData?.siblingsSearch?.searchResults?.map((result) => result.entity) || [];
};
