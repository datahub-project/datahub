import '@app/settingsV2/sample-data-notification.less';

import { Card, Icon, Loader, PageTitle, Pill, Switch, Text, colors } from '@components';
import { message, notification } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useGlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';
import { useUserContext } from '@app/context/useUserContext';
import OrganizationInfo from '@app/settingsV2/OrganizationInfo';
import { useAppConfig } from '@app/useAppConfig';
import { useGetAuthenticatedUser } from '@app/useGetAuthenticatedUser';
import { useIsThemeV2, useIsThemeV2EnabledForUser, useIsThemeV2Toggleable } from '@app/useIsThemeV2';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

import { useUpdateApplicationsSettingsMutation } from '@graphql/app.generated';
import { useUpdateUserSettingMutation } from '@graphql/me.generated';
import { useUpdateSampleDataSettingsMutation } from '@graphql/settings.generated';
import { UserSetting } from '@types';

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
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px;
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
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
    color: ${colors.gray[600]};
    font-weight: 700;
`;

const DescriptionText = styled(Text)`
    color: ${colors.gray[1700]};
    font-size: 14px;
    font-weight: 400;
    line-height: 1.5;
`;

const ChevronButton = styled.div`
    display: flex;
    align-items: center;
    cursor: pointer;
    gap: 4px;
`;

const ChevronIcon = styled(Icon)`
    color: ${colors.gray[1700]};
    font-size: 12px;
`;

const BetaFeaturesHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
`;

const BetaFeaturesTitle = styled.div`
    font-size: 16px;
    color: ${colors.gray[600]};
    font-weight: 700;
    gap: 8px;
    display: flex;
`;

export const Preferences = () => {
    // Current User Urn
    const { user, refetchUser } = useUserContext();
    const [isBetaFeaturesExpanded, setIsBetaFeaturesExpanded] = useState(false);
    const isThemeV2 = useIsThemeV2();
    const [isThemeV2Toggleable] = useIsThemeV2Toggleable();
    const [isThemeV2EnabledForUser] = useIsThemeV2EnabledForUser();
    const userContext = useUserContext();
    const appConfig = useAppConfig();
    const { globalSettings, refetch: refetchGlobalSettings } = useGlobalSettingsContext();

    const showSimplifiedHomepage = !!user?.settings?.appearance?.showSimplifiedHomepage;

    const applicationsEnabled = appConfig.config?.visualConfig?.application?.showApplicationInNavigation ?? false;

    const [updateUserSettingMutation] = useUpdateUserSettingMutation();
    const [updateApplicationsSettingsMutation] = useUpdateApplicationsSettingsMutation();
    const [updateSampleDataSettingsMutation] = useUpdateSampleDataSettingsMutation();
    const [isSampleDataToggleLoading, setIsSampleDataToggleLoading] = useState(false);

    const showSimplifiedHomepageSetting = !isThemeV2;
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const authenticatedUser = useGetAuthenticatedUser();

    const canManageGlobalSettings = authenticatedUser?.platformPrivileges?.manageGlobalSettings;
    const canManageOrganizationDisplayPreferences =
        authenticatedUser?.platformPrivileges?.manageOrganizationDisplayPreferences;
    const canManageApplicationAppearance = userContext?.platformPrivileges?.manageFeatures;
    // only show beta features card if the user has access (currently only applications)
    const anyBetaFeaturesEnabled = canManageApplicationAppearance;

    const isFreeTrialInstance = appConfig.config.trialConfig?.trialEnabled ?? false;
    const showSampleDataToggle = isFreeTrialInstance && canManageGlobalSettings;
    const sampleDataEnabled = globalSettings?.visualSettings?.sampleDataSettings?.enabled ?? false;

    return (
        <Page>
            <SourceContainer>
                <TokensContainer>
                    <HeaderContainer>
                        <PageTitle title="Appearance" subTitle="Manage your appearance settings." />
                    </HeaderContainer>
                </TokensContainer>
                {canManageOrganizationDisplayPreferences && isShowNavBarRedesign && <OrganizationInfo />}
                {showSimplifiedHomepageSetting && (
                    <StyledCard>
                        <UserSettingRow>
                            <TextContainer>
                                <SettingText>Show simplified homepage </SettingText>
                                <DescriptionText>
                                    Limits entity browse cards on homepage to Domains, Charts, Datasets, Dashboards and
                                    Glossary Terms
                                </DescriptionText>
                            </TextContainer>
                            <Switch
                                label=""
                                checked={showSimplifiedHomepage}
                                onChange={async () => {
                                    await updateUserSettingMutation({
                                        variables: {
                                            input: {
                                                name: UserSetting.ShowSimplifiedHomepage,
                                                value: !showSimplifiedHomepage,
                                            },
                                        },
                                    });
                                    analytics.event({
                                        type: showSimplifiedHomepage
                                            ? EventType.ShowStandardHomepageEvent
                                            : EventType.ShowSimplifiedHomepageEvent,
                                    });
                                    message.success({ content: 'Setting updated!', duration: 2 });
                                    refetchUser?.();
                                }}
                            />
                        </UserSettingRow>
                    </StyledCard>
                )}
                {showSampleDataToggle && (
                    <StyledCard>
                        <UserSettingRow>
                            <TextContainer>
                                <SettingText>Use Sample Data</SettingText>
                                <DescriptionText>
                                    Sample data is pre-loaded as part of this free trial. It appears in Search, Lineage,
                                    Ask DataHub, and everywhere else.
                                </DescriptionText>
                            </TextContainer>
                            {isSampleDataToggleLoading ? (
                                <div style={{ width: '44px', display: 'flex', justifyContent: 'center' }}>
                                    <Loader size="sm" />
                                </div>
                            ) : (
                                <Switch
                                    label=""
                                    checked={sampleDataEnabled}
                                    onChange={async () => {
                                        const newEnabled = !sampleDataEnabled;
                                        setIsSampleDataToggleLoading(true);
                                        try {
                                            await updateSampleDataSettingsMutation({
                                                variables: {
                                                    input: {
                                                        enabled: newEnabled,
                                                    },
                                                },
                                            });
                                            notification.open({
                                                message: (
                                                    <Text color="violet" colorLevel={500} weight="semiBold">
                                                        Sample data {newEnabled ? 'enabled' : 'disabled'}. Changes may
                                                        take a few minutes.
                                                    </Text>
                                                ),
                                                icon: (
                                                    <Icon
                                                        icon="MegaphoneSimple"
                                                        weight="fill"
                                                        source="phosphor"
                                                        color="violet"
                                                    />
                                                ),
                                                closeIcon: (
                                                    <Icon
                                                        icon="X"
                                                        source="phosphor"
                                                        color="violet"
                                                        colorLevel={500}
                                                        size="lg"
                                                    />
                                                ),
                                                style: {
                                                    backgroundColor: colors.violet[0],
                                                    borderRadius: 8,
                                                    width: 510,
                                                },
                                                duration: 4,
                                                placement: 'top',
                                            });
                                            refetchGlobalSettings?.();
                                        } catch (error: any) {
                                            notification.error({
                                                message: 'Failed to update sample data setting',
                                                description: error?.message || 'Please try again in a few moments.',
                                                duration: 5,
                                                placement: 'top',
                                            });
                                        } finally {
                                            setIsSampleDataToggleLoading(false);
                                        }
                                    }}
                                />
                            )}
                        </UserSettingRow>
                    </StyledCard>
                )}
                {isThemeV2Toggleable && (
                    <>
                        <StyledCard>
                            <UserSettingRow>
                                <span>
                                    <SettingText>Try DataHub 2.0</SettingText>
                                    <div>
                                        <DescriptionText>
                                            Enable an early preview of DataHub 2.0 - a complete makeover for your app
                                            with a sleek new design and advanced features. Flip the switch and refresh
                                            your browser to try it out!
                                        </DescriptionText>
                                    </div>
                                </span>
                                <Switch
                                    label=""
                                    checked={isThemeV2EnabledForUser}
                                    onChange={async () => {
                                        await updateUserSettingMutation({
                                            variables: {
                                                input: {
                                                    name: UserSetting.ShowThemeV2,
                                                    value: !isThemeV2EnabledForUser,
                                                },
                                            },
                                        });
                                        // clicking this button toggles, so event is whatever is opposite to what isThemeV2EnabledForUser currently is
                                        analytics.event({
                                            type: isThemeV2EnabledForUser
                                                ? EventType.RevertV2ThemeEvent
                                                : EventType.ShowV2ThemeEvent,
                                        });
                                        message.success({ content: 'Setting updated!', duration: 2 });
                                        refetchUser?.();
                                    }}
                                />
                            </UserSettingRow>
                        </StyledCard>
                    </>
                )}
                {!showSimplifiedHomepageSetting &&
                    !isThemeV2Toggleable &&
                    !canManageOrganizationDisplayPreferences &&
                    !canManageApplicationAppearance && (
                        <div style={{ color: colors.gray[1700] }}>No appearance settings found.</div>
                    )}
                {anyBetaFeaturesEnabled && (
                    <Card
                        title={
                            <BetaFeaturesHeader>
                                <BetaFeaturesTitle>
                                    Beta Features
                                    <Pill color="violet" variant="filled" label="Experimental" size="sm" />
                                </BetaFeaturesTitle>
                                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                    <ChevronButton onClick={() => setIsBetaFeaturesExpanded(!isBetaFeaturesExpanded)}>
                                        <ChevronIcon
                                            icon={isBetaFeaturesExpanded ? 'CaretUp' : 'CaretDown'}
                                            source="phosphor"
                                            size="md"
                                        />
                                    </ChevronButton>
                                </div>
                            </BetaFeaturesHeader>
                        }
                        subTitle="These features are under active development and may change."
                    >
                        {isBetaFeaturesExpanded && (
                            <div style={{ marginTop: 16 }}>
                                {canManageApplicationAppearance && (
                                    <StyledCard>
                                        <UserSettingRow>
                                            <TextContainer>
                                                <SettingText>Show Applications</SettingText>
                                                <DescriptionText>
                                                    Applications are another way to organize your data, similar to
                                                    Domains. This is a Global setting that will affect all users in your
                                                    organization.
                                                </DescriptionText>
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
                                                    message.success({ content: 'Setting updated!', duration: 2 });
                                                    appConfig?.refreshContext();
                                                }}
                                            />
                                        </UserSettingRow>
                                    </StyledCard>
                                )}
                            </div>
                        )}
                    </Card>
                )}
            </SourceContainer>
        </Page>
    );
};
