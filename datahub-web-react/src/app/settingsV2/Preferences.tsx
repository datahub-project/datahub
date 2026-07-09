import { PageTitle, Switch } from '@components';
import { message } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { LanguageSelect } from '@app/i18n/components/LanguageSelect';
import { useIsI18nEnabled } from '@app/i18n/hooks/useIsI18nEnabled';
import { useAppConfig } from '@app/useAppConfig';

import { useUpdateApplicationsSettingsMutation } from '@graphql/app.generated';

const Page = styled.div`
    width: 100%;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const HeaderContainer = styled.div`
    margin-bottom: 24px;
`;

const StyledCard = styled.div`
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 12px;
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    padding: 16px;
    display: flex;
    justify-content: space-between;
    margin-bottom: 16px;
`;

const SourceContainer = styled.div`
    width: 100%;
    padding: 16px 20px 16px 20px;
`;

const TokensContainer = styled.div`
    padding-top: 0px;
`;

const TextContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

const UserSettingRow = styled.div`
    display: flex;
    justify-content: space-between;
    flex-direction: row;
    align-items: center;
    width: 100%;
`;

const SettingText = styled.div`
    font-size: 16px;
    color: ${(props) => props.theme.colors.text};
    font-weight: 700;
`;

const DescriptionText = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 14px;
    font-weight: 400;
    line-height: 1.5;
`;

export const Preferences = () => {
    const { t } = useTranslation('settings.preferences');
    const theme = useTheme();
    const userContext = useUserContext();
    const appConfig = useAppConfig();
    const i18nEnabled = useIsI18nEnabled();

    const applicationsEnabled = appConfig.config?.visualConfig?.application?.showApplicationInNavigation ?? false;

    const [updateApplicationsSettingsMutation] = useUpdateApplicationsSettingsMutation();

    const canManageApplicationAppearance = userContext?.platformPrivileges?.manageFeatures;

    return (
        <Page>
            <SourceContainer>
                <TokensContainer>
                    <HeaderContainer>
                        <PageTitle title={t('appearance.title')} subTitle={t('appearance.subTitle')} />
                    </HeaderContainer>
                </TokensContainer>
                {canManageApplicationAppearance && (
                    <StyledCard>
                        <UserSettingRow>
                            <TextContainer>
                                <SettingText>{t('showApplications.title')}</SettingText>
                                <DescriptionText>{t('showApplications.description')}</DescriptionText>
                            </TextContainer>
                            <Switch
                                label=""
                                checked={applicationsEnabled}
                                onChange={async () => {
                                    await updateApplicationsSettingsMutation({
                                        variables: {
                                            input: {
                                                enabled: !applicationsEnabled,
                                            },
                                        },
                                    });
                                    message.success({
                                        content: t('showApplications.successMessage'),
                                        duration: 2,
                                    });
                                    appConfig?.refreshContext();
                                }}
                            />
                        </UserSettingRow>
                    </StyledCard>
                )}
                {i18nEnabled && (
                    <StyledCard>
                        <UserSettingRow>
                            <TextContainer>
                                <SettingText>{t('language.title')}</SettingText>
                                <DescriptionText>{t('language.description')}</DescriptionText>
                            </TextContainer>
                            <LanguageSelect />
                        </UserSettingRow>
                    </StyledCard>
                )}
                {!canManageApplicationAppearance && !i18nEnabled && (
                    <div style={{ color: theme.colors.textSecondary }}>{t('noSettings')}</div>
                )}
            </SourceContainer>
        </Page>
    );
};
