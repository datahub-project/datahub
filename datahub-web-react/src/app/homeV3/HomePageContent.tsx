import React from 'react';

import { useGlobalSettings } from '@app/context/GlobalSettingsContext';
import { useUserContext } from '@app/context/useUserContext';
import { Announcements } from '@app/homeV3/announcements/Announcements';
import { CenteredContainer, ContentContainer, ContentDiv } from '@app/homeV3/styledComponents';
import TemplateRow from '@app/homeV3/templateRow/TemplateRow';

const HomePageContent = () => {
    const { settings } = useGlobalSettings();
    const { user } = useUserContext();

    const template = user?.settings?.homePage?.pageTemplate || settings.globalHomePageSettings?.defaultTemplate;

    return (
        <ContentContainer>
            <CenteredContainer>
                <ContentDiv>
                    <Announcements />
                    {template?.properties.rows.map((row, i) => {
                        const key = `templateRow-${i}`;
                        return <TemplateRow key={key} row={row} />;
                    })}
                </ContentDiv>
            </CenteredContainer>
        </ContentContainer>
    );
};

export default HomePageContent;
