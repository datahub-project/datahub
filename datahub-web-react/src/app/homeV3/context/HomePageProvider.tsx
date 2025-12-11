/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { PageTemplateProvider } from '@app/homeV3/context/PageTemplateContext';

import { PageTemplateSurfaceType } from '@types';

const HomePageProvider = ({ children }: { children: React.ReactNode }) => {
    return <PageTemplateProvider templateType={PageTemplateSurfaceType.HomePage}>{children}</PageTemplateProvider>;
};

export default HomePageProvider;
