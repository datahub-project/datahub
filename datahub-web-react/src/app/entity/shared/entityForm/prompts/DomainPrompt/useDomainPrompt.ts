import { useMemo, useState } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { getPromptAssociation } from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import { FormView, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { getDefaultDomain } from '@app/entity/shared/entityForm/prompts/DomainPrompt/utils';

import { FormPrompt, FormPromptType, SubmitFormPromptInput } from '@types';

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
                ? promptAssociation?.response?.domainResponse?.domain || getDefaultDomain(entityData, prompt)
                : null,
        [formView, promptAssociation?.response?.domainResponse?.domain, entityData, prompt],
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
