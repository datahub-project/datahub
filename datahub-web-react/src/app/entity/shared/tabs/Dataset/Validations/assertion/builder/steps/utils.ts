import { AssertionActionType, AssertionType } from '../../../../../../../../../types.generated';
import { AssertionMonitorBuilderState } from '../types';

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
        default:
            throw new Error(`Unknown assertion type: ${assertionType}`);
    }
};
