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
import { Assertion, AssertionResultType, AssertionType, EntityType } from '../../../../../../types.generated';
import { sortAssertions } from './assertionUtils';
import { AssertionGroup, AssertionStatusSummary } from './types';
import { lowerFirstLetter } from '../../../../../shared/textUtil';

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

export const getAssertionTypesForEntityType = (entityType: EntityType, monitorsConnectionForEntityExists: boolean) => {
    return ASSERTION_INFO.filter((type) => type.entityTypes.includes(entityType)).map((type) => ({
        ...type,
        enabled: type.enabled && (!type.requiresConnectionSupportedByMonitors || monitorsConnectionForEntityExists),
    }));
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
