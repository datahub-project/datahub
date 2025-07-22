import React from 'react';

import { useGlobalSettings } from '@app/context/GlobalSettingsContext';
import { useUserContext } from '@app/context/useUserContext';
import { PageTemplateProvider } from '@app/homeV3/context/PageTemplateContext';

const HomePageProvider = ({ children }: { children: React.ReactNode }) => {
    const { settings, loaded: globalSettingsLoaded } = useGlobalSettings();
    const { user, loaded: userLoaded } = useUserContext();

    const personalTemplate = user?.settings?.homePage?.pageTemplate || null;
    const globalTemplate = settings.globalHomePageSettings?.defaultTemplate || null;

    if (!userLoaded || !globalSettingsLoaded) return null;
    return (
        <PageTemplateProvider personalTemplate={personalTemplate} globalTemplate={globalTemplate}>
            {children}
        </PageTemplateProvider>
    );
};

export default HomePageProvider;
