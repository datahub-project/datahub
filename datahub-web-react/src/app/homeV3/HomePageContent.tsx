/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { Announcements } from '@app/homeV3/announcements/Announcements';
import EditDefaultTemplateBar from '@app/homeV3/settings/EditDefaultTemplateBar';
import HomePageSettingsButtonWrapper from '@app/homeV3/settings/HomePageSettingsButtonWrapper';
import { CenteredContainer, ContentContainer, ContentDiv } from '@app/homeV3/styledComponents';
import Template from '@app/homeV3/template/Template';

const HomePageContent = () => {
    return (
        <ContentContainer>
            <CenteredContainer>
                <ContentDiv>
                    <HomePageSettingsButtonWrapper />
                    <Announcements />
                    <Template />
                    <EditDefaultTemplateBar />
                </ContentDiv>
            </CenteredContainer>
        </ContentContainer>
    );
};

export default HomePageContent;
