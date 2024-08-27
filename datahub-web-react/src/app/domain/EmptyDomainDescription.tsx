import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { useTranslation } from 'react-i18next';

const StyledParagraph = styled(Typography.Paragraph)`
    text-align: justify;
    text-justify: inter-word;
    margin: 40px 0;
    font-size: 15px;
`;

function EmptyDomainDescription() {
    const { t } = useTranslation();
    return (
        <>
            <StyledParagraph type="secondary">{t('domain.domainDescriptionWelcome')}</StyledParagraph>
            <StyledParagraph type="secondary">{t('domain.domainDescriptionCreateDomains')}</StyledParagraph>
            <StyledParagraph type="secondary">{t('domain.domainDescriptionBuildProducts')}</StyledParagraph>
            <StyledParagraph type="secondary">{t('domain.domainDescriptionCallToAction')}</StyledParagraph>
        </>
    );
}

export default EmptyDomainDescription;
