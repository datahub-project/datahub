import React from 'react';

import { useGlobalSettings } from '@app/context/GlobalSettingsContext';
import { useUserContext } from '@app/context/useUserContext';
import { ContentContainer, ContentWrapper, StyledVectorBackground } from '@app/homepageV2/styledComponents';
import TemplateRow from '@app/homepageV2/templateRow/TemplateRow';

const HomePageContent = () => {
    const { settings } = useGlobalSettings();
    const { user } = useUserContext();

    const template = user?.settings?.homePage?.pageTemplate || settings.globalHomePageSettings?.defaultTemplate;

    return (
        <ContentWrapper>
            <StyledVectorBackground />
            <ContentContainer>
                {template?.properties.rows.map((row, i) => {
                    const key = `templateRow-${i}`;
                    return <TemplateRow key={key} row={row} />;
                })}
            </ContentContainer>
        </ContentWrapper>
    );
};

export default HomePageContent;
