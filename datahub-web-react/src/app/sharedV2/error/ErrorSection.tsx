import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { Text } from '@components/components/Text';

import dataHubLogo from '@images/datahublogo.png';

const Section = styled.div`
    width: auto;
    margin-top: 40px;
    margin-left: 60px;
`;

// The routes' error fallback loads with the app shell, so it uses plain elements: antd's Image and
// Typography would pull their preview, tooltip and editable-input code into the eager bundle
const Logo = styled.img`
    width: 40px;
`;

const TitleText = styled.span`
    font-size: 18px;
    font-weight: 600;
    margin-left: 8px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const Title = styled.h2`
    font-size: 26px;
    font-weight: 700;
    line-height: 1.35;
    margin-bottom: 0.5em;
    color: ${(props) => props.theme.colors.text};
`;

const TitleSection = styled.div`
    display: flex;
    align-items: center;
    margin-bottom: 20px;
`;

const MessageSection = styled.div`
    margin-bottom: 20px;
`;

const DetailParagraph = styled(Text)`
    font-size: 14px;
`;

const ResourceList = styled.ul`
    font-size: 14px;
`;

const ResourceListItem = styled.li`
    margin-bottom: 4px;
`;

const resources = [
    {
        /* untranslated-text */
        label: 'DataHub Project',
        path: 'https://docs.datahub.com',
        shouldOpenInNewTab: true,
        description: 'DataHub Project website',
    },
    {
        /* untranslated-text */
        label: 'DataHub Docs',
        path: 'https://docs.datahub.com/docs',
        shouldOpenInNewTab: true,
    },
    {
        /* untranslated-text */
        label: 'DataHub GitHub',
        path: 'https://github.com/datahub-project/datahub',
        shouldOpenInNewTab: true,
    },
];

export const ErrorSection = (): JSX.Element => {
    const { t } = useTranslation('shared.error');
    const themeConfig = useTheme();
    const themeLogo = themeConfig.assets.logoUrl || dataHubLogo;

    return (
        <Section>
            <div>
                <TitleSection>
                    <Logo src={themeLogo} alt="" />
                    <TitleText>{themeConfig.content.title}</TitleText>
                </TitleSection>
                <MessageSection>
                    <Title>{t('section.title')}</Title>
                    <DetailParagraph color="textSecondary">{t('section.description')}</DetailParagraph>
                </MessageSection>
                <div>
                    <DetailParagraph color="textSecondary">
                        <Trans t={t} i18nKey="section.needSupport" components={{ bold: <b /> }} />
                    </DetailParagraph>
                    <ResourceList>
                        {resources.map((resource) => (
                            <ResourceListItem key={resource.path}>
                                <a href={resource.path}>{resource.label}</a>
                            </ResourceListItem>
                        ))}
                    </ResourceList>
                </div>
            </div>
        </Section>
    );
};
