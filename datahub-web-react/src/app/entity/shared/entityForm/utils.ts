import { FormResponsesFilter, FormView } from './EntityFormContext';

export function generateFormResponsesFilters(
    formView: FormView,
    selectedPromptId: string | null,
    formResponsesFilters: FormResponsesFilter[],
) {
    if (!selectedPromptId || formView !== FormView.BY_QUESTION) {
        return {};
    }

    if (formResponsesFilters.length === 1 && formResponsesFilters.includes(FormResponsesFilter.INCOMPLETE)) {
        return { promptId: selectedPromptId, isPromptComplete: false };
    }

    if (formResponsesFilters.length === 1 && formResponsesFilters.includes(FormResponsesFilter.COMPLETE)) {
        return { promptId: selectedPromptId, isPromptComplete: true };
    }

    return {};
}

export function generateFormCompletionFilter(formView: FormView, isVerificationType: boolean) {
    if (formView === FormView.BULK_VERIFY) {
        return { isFormComplete: true, isFormVerified: false };
    }

    return isVerificationType ? { isFormVerified: false } : { isFormComplete: false };
}
