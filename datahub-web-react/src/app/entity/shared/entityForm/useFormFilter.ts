import { useState } from 'react';
import { FormResponsesFilter, FormView } from './EntityFormContext';
import { generateFormCompletionFilter, generateFormResponsesFilters } from './utils';
import { useUserContext } from '../../../context/useUserContext';

interface Props {
    formUrn: string;
    isVerificationType: boolean;
    formView: FormView;
    selectedPromptId: string | null;
}

export default function useFormFilter({ formUrn, isVerificationType, formView, selectedPromptId }: Props) {
    const { user } = useUserContext();
    const [formResponsesFilters, setFormResponsesFilters] = useState<FormResponsesFilter[]>([
        FormResponsesFilter.INCOMPLETE,
    ]);

    const formCompletionFilter = generateFormCompletionFilter(formView, isVerificationType);
    const promptCompletionFilter = generateFormResponsesFilters(formView, selectedPromptId, formResponsesFilters);
    const formFilter = { formUrn, assignedActor: user?.urn, ...formCompletionFilter, ...promptCompletionFilter };

    return {
        formFilter,
        formResponsesFilters,
        setFormResponsesFilters,
    };
}
