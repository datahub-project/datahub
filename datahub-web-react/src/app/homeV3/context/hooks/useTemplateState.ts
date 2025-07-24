import { useEffect, useMemo, useState } from 'react';

import { useGlobalSettings } from '@app/context/GlobalSettingsContext';
import { useUserContext } from '@app/context/useUserContext';

import { PageTemplateFragment } from '@graphql/template.generated';

export function useTemplateState() {
    const [areTemplatesInitialized, setAreTemplatedInitialized] = useState(false);
    const [personalTemplate, setPersonalTemplate] = useState<PageTemplateFragment | null>(null);
    const [globalTemplate, setGlobalTemplate] = useState<PageTemplateFragment | null>(null);

    const { settings, loaded: globalSettingsLoaded } = useGlobalSettings();
    const { user, loaded: userLoaded } = useUserContext();

    useEffect(() => {
        if (globalSettingsLoaded && userLoaded && !areTemplatesInitialized) {
            setGlobalTemplate(settings.globalHomePageSettings?.defaultTemplate || null);
            setPersonalTemplate(user?.settings?.homePage?.pageTemplate || null);
            setAreTemplatedInitialized(true);
        }
    }, [
        globalSettingsLoaded,
        userLoaded,
        areTemplatesInitialized,
        settings.globalHomePageSettings?.defaultTemplate,
        user?.settings?.homePage?.pageTemplate,
    ]);

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
