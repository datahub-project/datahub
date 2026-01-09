import {
    EXECUTION_REQUEST_STATUS_FAILURE,
    EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS,
    EXECUTION_REQUEST_STATUS_SUCCESS,
} from '@app/ingestV2/executions/constants';

export const getSuggestedQuestions = (status: string) => {
    if (status === EXECUTION_REQUEST_STATUS_SUCCESS) {
        return [
            'Summarize what was ingested from this run',
            'How can I improve performance for this source?',
            'What source should I connect next?',
        ];
    }
    if (status === EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS) {
        return ['Summarize what happened', 'What is the impact?', 'What should I resolve before the next run?'];
    }
    if (status === EXECUTION_REQUEST_STATUS_FAILURE) {
        return [
            'What’s the main error?',
            'What should I check in my configuration?',
            'Walk me through fixing this step-by-step',
        ];
    }
    return [
        'What’s the current status of this source?',
        'Should I change any settings for best results?',
        'What should I do next?',
    ];
};
