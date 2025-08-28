import React from 'react';

import { PageTemplateProvider } from '@app/homeV3/context/PageTemplateContext';
import Template from '@app/homeV3/template/Template';

import { PageTemplateSurfaceType } from '@types';

export default function SummaryTabTemplate() {
    return (
        <PageTemplateProvider templateType={PageTemplateSurfaceType.AssetSummary}>
            <Template />
        </PageTemplateProvider>
    );
}
