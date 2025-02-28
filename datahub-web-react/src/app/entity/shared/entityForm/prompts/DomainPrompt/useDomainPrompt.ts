import { useMemo, useState } from 'react';
import { useEntityData } from '../../../EntityContext';
import { FormPrompt, FormPromptType, SubmitFormPromptInput } from '../../../../../../types.generated';
import { FormView, useEntityFormContext } from '../../EntityFormContext';
import { getPromptAssociation } from '../../../containers/profile/sidebar/FormInfo/utils';

interface Props {
    prompt: FormPrompt;
    submitResponse: (input: SubmitFormPromptInput, onSuccess: () => void) => void;
}

export default function useDomainPrompt({ prompt, submitResponse }: Props) {
    const [hasEdited, setHasEdited] = useState(false);
    const { entityData } = useEntityData();
    const promptAssociation = getPromptAssociation(entityData, prompt.id);

    const {
        form: { formView },
    } = useEntityFormContext();

    const initialEntity = useMemo(
        () =>
            formView === FormView.BY_ENTITY
                ? promptAssociation?.response?.domainResponse?.domain || entityData?.domain?.domain || null
                : null,
        [formView, promptAssociation?.response?.domainResponse?.domain, entityData?.domain?.domain],
    );

    const [selectedDomain, setSelectedDomain] = useState<string | null>(initialEntity?.urn || null);

    function updateSelectedDomain(domainUrn: string) {
        setSelectedDomain(domainUrn);
        setHasEdited(true);
    }

    function submitDomainResponse() {
        if (selectedDomain) {
            submitResponse(
                {
                    promptId: prompt.id,
                    formUrn: prompt.formUrn,
                    type: FormPromptType.Domain,
                    domainParams: {
                        domainUrn: selectedDomain,
                    },
                },
                () => {
                    setHasEdited(false);
                },
            );
        }
    }

    return {
        hasEdited,
        selectedDomain,
        initialEntity,
        submitDomainResponse,
        updateSelectedDomain,
    };
}
