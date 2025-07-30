import React from 'react';

import { PageTemplateProvider } from '@app/homeV3/context/PageTemplateContext';

const HomePageProvider = ({ children }: { children: React.ReactNode }) => {
    return <PageTemplateProvider>{children}</PageTemplateProvider>;
};

export default HomePageProvider;
