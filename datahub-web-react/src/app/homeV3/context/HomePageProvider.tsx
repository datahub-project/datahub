import React from 'react';

import { PageTemplateProvider } from '@app/homeV3/context/PageTemplateContext';

import { PageTemplateSurfaceType } from '@types';

const HomePageProvider = ({ children }: { children: React.ReactNode }) => {
    return <PageTemplateProvider templateType={PageTemplateSurfaceType.HomePage}>{children}</PageTemplateProvider>;
};

export default HomePageProvider;
