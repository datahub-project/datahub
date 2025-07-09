import { useMemo, useState } from 'react';

import { PageTemplateFragment } from '@graphql/template.generated';

export function useTemplateState(
    initialPersonalTemplate: PageTemplateFragment | null | undefined,
    initialGlobalTemplate: PageTemplateFragment | null | undefined,
) {
    const [personalTemplate, setPersonalTemplate] = useState<PageTemplateFragment | null>(
        initialPersonalTemplate || null,
    );
    const [globalTemplate, setGlobalTemplate] = useState<PageTemplateFragment | null>(
        initialGlobalTemplate || null,
    );
    const [isEditingGlobalTemplate, setIsEditingGlobalTemplate] = useState(false);

    // The current template is personal unless editing global or personal is missing
    const template = useMemo(
        () => (isEditingGlobalTemplate ? globalTemplate : personalTemplate || globalTemplate),
        [isEditingGlobalTemplate, personalTemplate, globalTemplate],
    );

    const setTemplate = (t: PageTemplateFragment | null) => {
        if (isEditingGlobalTemplate) {
            setGlobalTemplate(t);
        } else {
            setPersonalTemplate(t);
        }
    };

    return {
        personalTemplate,
        globalTemplate,
        template,
        isEditingGlobalTemplate,
        setIsEditingGlobalTemplate,
        setPersonalTemplate,
        setGlobalTemplate,
        setTemplate,
    };
} 