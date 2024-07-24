import React from 'react';
import styled from 'styled-components';
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
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionType,
    AssertionValueChangeType,
    EntityType,
    VolumeAssertionInfo,
    VolumeAssertionType,
} from '../../../../../../types.generated';
import { sortAssertions } from './assertionUtils';
import { AssertionGroup, AssertionStatusSummary } from './types';
import { formatNumberWithoutAbbreviation } from '../../../../../shared/formatNumber';
import { parseMaybeStringAsFloatOrDefault } from '../../../../../shared/numberUtil';

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

// /**
//  * Returns a list of assertion groups, where assertions are grouped
//  * by their "type" or "category". Each group includes the assertions inside, along with
//  * a summary of passing and failing assertions for the group.
//  *
//  * @param assertions The assertions to group
//  */
export const createAssertionGroups = (assertions: Array<Assertion>): AssertionGroup[] => {
    // Pre-sort the list of assertions based on which has been most recently executed.
    const newAssertions = [...assertions].sort(sortAssertions);

    const typeToAssertions = new Map();
    newAssertions
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

type VolumeTypeField =
    | 'rowCountTotal'
    | 'rowCountChange'
    | 'incrementingSegmentRowCountTotal'
    | 'incrementingSegmentRowCountChange';

export const getPropertyFromVolumeType = (type: VolumeAssertionType) => {
    switch (type) {
        case VolumeAssertionType.RowCountTotal:
            return 'rowCountTotal' as VolumeTypeField;
        case VolumeAssertionType.RowCountChange:
            return 'rowCountChange' as VolumeTypeField;
        case VolumeAssertionType.IncrementingSegmentRowCountTotal:
            return 'incrementingSegmentRowCountTotal' as VolumeTypeField;
        case VolumeAssertionType.IncrementingSegmentRowCountChange:
            return 'incrementingSegmentRowCountChange' as VolumeTypeField;
        default:
            throw new Error(`Unknown volume assertion type: ${type}`);
    }
};

export const getVolumeTypeInfo = (volumeAssertion: VolumeAssertionInfo) => {
    const result = volumeAssertion[getPropertyFromVolumeType(volumeAssertion.type)];
    if (!result) {
        return undefined;
    }
    return result;
};

export const getIsRowCountChange = (type: VolumeAssertionType) => {
    return [VolumeAssertionType.RowCountChange, VolumeAssertionType.IncrementingSegmentRowCountChange].includes(type);
};

export const getVolumeTypeDescription = (volumeType: VolumeAssertionType) => {
    switch (volumeType) {
        case VolumeAssertionType.RowCountTotal:
        case VolumeAssertionType.IncrementingSegmentRowCountTotal:
            return 'has';
        case VolumeAssertionType.RowCountChange:
        case VolumeAssertionType.IncrementingSegmentRowCountChange:
            return 'should grow by';
        default:
            throw new Error(`Unknown volume type ${volumeType}`);
    }
};

export const getOperatorDescription = (operator: AssertionStdOperator) => {
    switch (operator) {
        case AssertionStdOperator.GreaterThanOrEqualTo:
            return 'at least';
        case AssertionStdOperator.LessThanOrEqualTo:
            return 'at most';
        case AssertionStdOperator.Between:
            return 'between';
        default:
            throw new Error(`Unknown operator ${operator}`);
    }
};

export const getValueChangeTypeDescription = (valueChangeType: AssertionValueChangeType) => {
    switch (valueChangeType) {
        case AssertionValueChangeType.Absolute:
            return 'rows';
        case AssertionValueChangeType.Percentage:
            return '%';
        default:
            throw new Error(`Unknown value change type ${valueChangeType}`);
    }
};

export const getParameterDescription = (parameters: AssertionStdParameters) => {
    if (parameters.value) {
        return formatNumberWithoutAbbreviation(
            parseMaybeStringAsFloatOrDefault(parameters.value.value, parameters.value.value),
        );
    }
    if (parameters.minValue && parameters.maxValue) {
        return `${formatNumberWithoutAbbreviation(
            parseMaybeStringAsFloatOrDefault(parameters.minValue.value, parameters.minValue.value),
        )} and ${formatNumberWithoutAbbreviation(
            parseMaybeStringAsFloatOrDefault(parameters.maxValue.value, parameters.maxValue.value),
        )}`;
    }
    throw new Error('Invalid assertion parameters provided');
};
