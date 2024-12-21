import { AssertionActionType, AssertionType } from '@src/types.generated';
import { Form } from 'antd';
import { useState } from 'react';
import {
    DEFAULT_DATASET_FRESHNESS_ASSERTION_STATE,
    DEFAULT_DATASET_SQL_ASSERTION_PARAMETERS_STATE,
    DEFAULT_DATASET_SQL_ASSERTION_STATE,
    DEFAULT_DATASET_VOLUME_ASSERTION_STATE,
} from '../constants';
import { AssertionMonitorBuilderState } from '../types';
import { getDefaultDatasetFreshnessAssertionParametersState } from '../utils';
import {
    getDefaultDatasetFieldAssertionParametersState,
    getDefaultDatasetFieldAssertionState,
    getDefaultDatasetSchemaAssertionParametersState,
    getDefaultDatasetSchemaAssertionState,
} from './field/utils';
import { getDefaultDatasetVolumeAssertionParametersState } from './volume/utils';

export const toggleRaiseIncidentState = (state: AssertionMonitorBuilderState, newValue: boolean) => {
    let newFailureActions = state?.assertion?.actions?.onFailure || [];
    if (newValue === true) {
        // Add auto-raise incident action.
        newFailureActions = [...newFailureActions, { type: AssertionActionType.RaiseIncident }];
    } else {
        // Remove auto-raise incident actions.
        newFailureActions = [
            ...newFailureActions.filter((action) => action.type !== AssertionActionType.RaiseIncident),
        ];
    }
    return {
        ...state,
        assertion: {
            ...state.assertion,
            actions: {
                onSuccess: state.assertion?.actions?.onSuccess || [],
                onFailure: newFailureActions,
            },
        },
    };
};

export const toggleResolveIncidentState = (state: AssertionMonitorBuilderState, newValue: boolean) => {
    let newSuccessActions = state.assertion?.actions?.onSuccess || [];
    if (newValue === true) {
        // Add auto-resolve incident action.
        newSuccessActions = [...newSuccessActions, { type: AssertionActionType.ResolveIncident }];
    } else {
        // Remove auto-raise incident actions.
        newSuccessActions = [
            ...newSuccessActions.filter((action) => action.type !== AssertionActionType.ResolveIncident),
        ];
    }
    return {
        ...state,
        assertion: {
            ...state.assertion,
            actions: {
                onFailure: state.assertion?.actions?.onFailure || [],
                onSuccess: newSuccessActions,
            },
        },
    };
};

export const updateExecutorIdState = (state: AssertionMonitorBuilderState, newValue: string) => {
    return {
        ...state,
        executorId: newValue,
    };
};

export const getEvaluationScheduleTooltipDescription = (assertionType: AssertionType, platformName: string) => {
    switch (assertionType) {
        case AssertionType.Freshness:
            return `At these times, we will determine the last time this dataset has changed. This may involve issuing a query to ${platformName}.`;
        case AssertionType.Volume:
            return `At these times, we will evaluate the row count for this dataset. This may involve issuing a query to ${platformName}.`;
        case AssertionType.Sql:
            return `At these times, we will evaluate the SQL query for this dataset. This involves issuing a query to ${platformName}.`;
        case AssertionType.Field:
            return `At these times, we will evaluate the field value for this dataset. This involves issuing a query to ${platformName}.`;
        case AssertionType.DataSchema:
            return `At these times, we will compare the actual recorded schema to the expected schema and report a pass or fail result. This does NOT involve querying ${platformName} directly.`;
        default:
            throw new Error(`Unknown assertion type: ${assertionType}`);
    }
};

export const getEvaluationScheduleTitle = (assertionType: AssertionType) => {
    switch (assertionType) {
        case AssertionType.Freshness:
            return 'Check for table changes';
        case AssertionType.Volume:
            return 'Check table volume';
        case AssertionType.Sql:
            return 'Run this query';
        case AssertionType.Field:
            return 'Check field value';
        case AssertionType.DataSchema:
            return 'Run schema comparison';
        default:
            throw new Error(`Unknown assertion type: ${assertionType}`);
    }
};

export const useTestAssertionModal = () => {
    const [isTestAssertionModalVisible, setTestAssertionModalVisible] = useState(false);
    const form = Form.useFormInstance();

    const handleTestAssertionSubmit = async () => {
        try {
            await form.validateFields();
            setTestAssertionModalVisible(true);
        } catch {
            // Ignore validation errors
        }
    };

    return {
        isTestAssertionModalVisible,
        handleTestAssertionSubmit,
        showTestAssertionModal: () => setTestAssertionModalVisible(true),
        hideTestAssertionModal: () => setTestAssertionModalVisible(false),
    };
};

export default function getInitBuilderStateByAssertionType(
    state: AssertionMonitorBuilderState,
    type: AssertionType,
    connectionForEntityExists: boolean,
    monitorsConnectionForEntityExists: boolean,
): AssertionMonitorBuilderState {
    switch (type) {
        case AssertionType.Freshness:
            return {
                ...state,
                assertion: {
                    type,
                    freshnessAssertion: DEFAULT_DATASET_FRESHNESS_ASSERTION_STATE,
                },
                parameters: getDefaultDatasetFreshnessAssertionParametersState(
                    state.platformUrn as string,
                    monitorsConnectionForEntityExists,
                ),
            };
        case AssertionType.Volume:
            return {
                ...state,
                assertion: {
                    type,
                    volumeAssertion: DEFAULT_DATASET_VOLUME_ASSERTION_STATE,
                },
                parameters: getDefaultDatasetVolumeAssertionParametersState(
                    state.platformUrn as string,
                    monitorsConnectionForEntityExists,
                ),
            };
        case AssertionType.Sql:
            return {
                ...state,
                assertion: {
                    type,
                    sqlAssertion: DEFAULT_DATASET_SQL_ASSERTION_STATE,
                },
                parameters: DEFAULT_DATASET_SQL_ASSERTION_PARAMETERS_STATE,
            };
        case AssertionType.Field:
            return {
                ...state,
                assertion: {
                    type,
                    fieldAssertion: getDefaultDatasetFieldAssertionState(connectionForEntityExists),
                },
                parameters: getDefaultDatasetFieldAssertionParametersState(connectionForEntityExists),
            };
        case AssertionType.DataSchema:
            return {
                ...state,
                assertion: {
                    type,
                    schemaAssertion: getDefaultDatasetSchemaAssertionState(),
                },
                parameters: getDefaultDatasetSchemaAssertionParametersState(),
            };
        default:
            return state;
    }
}
