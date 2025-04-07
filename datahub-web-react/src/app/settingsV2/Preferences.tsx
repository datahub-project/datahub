import React from 'react';
<<<<<<< HEAD
import { Card, message, Typography } from 'antd';
import { colors, PageTitle, Switch } from '@components';
||||||| 952f3cc3118
=======
import { message } from 'antd';
import { Switch, PageTitle, colors } from '@components';
>>>>>>> master
import styled from 'styled-components';
import { useUpdateUserSettingMutation } from '../../graphql/me.generated';
import { UserSetting } from '../../types.generated';
import analytics, { EventType } from '../analytics';
import { useUserContext } from '../context/useUserContext';
<<<<<<< HEAD
import { ANTD_GRAY } from '../entity/shared/constants';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
||||||| 952f3cc3118
import { ANTD_GRAY } from '../entity/shared/constants';
=======
>>>>>>> master
import { useIsThemeV2, useIsThemeV2EnabledForUser, useIsThemeV2Toggleable } from '../useIsThemeV2';
import OrganizationInfo from './OrganizationInfo';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';

const Page = styled.div`
    width: 100%;
    display: flex;
<<<<<<< HEAD
||||||| 952f3cc3118
    justify-content: center;
=======
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
>>>>>>> master
`;

const SourceContainer = styled.div`
<<<<<<< HEAD
    padding-top: 16px;
    padding-right: 20px;
    padding-left: 20px;
    width: 100%;
||||||| 952f3cc3118
    width: 80%;
    padding-top: 20px;
    padding-right: 40px;
    padding-left: 40px;
=======
    width: 100%;
    padding: 16px 20px 16px 20px;
>>>>>>> master
`;

const TokensContainer = styled.div`
    padding-top: 0px;
`;

<<<<<<< HEAD
||||||| 952f3cc3118
const TokensHeaderContainer = styled.div`
    && {
        padding-left: 0px;
    }
`;

const TokensTitle = styled(Typography.Title)`
    && {
        margin-bottom: 8px;
    }
`;

=======
const TextContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

>>>>>>> master
const UserSettingRow = styled.div`
    display: flex;
    justify-content: space-between;
    flex-direction: row;
    align-items: center;
    width: 100%;
`;

<<<<<<< HEAD
const DescriptionText = styled(Typography.Text)`
    color: ${colors.gray[1700]};
    font-size: 12px;
||||||| 952f3cc3118
const DescriptionText = styled(Typography.Text)`
    color: ${ANTD_GRAY[7]};
    font-size: 11px;
=======
const SettingText = styled.div`
    font-size: 16px;
    color: ${colors.gray[600]};
    font-weight: 700;
>>>>>>> master
`;

<<<<<<< HEAD
const SettingText = styled(Typography.Text)`
    font-size: 16px;
    color: ${colors.gray[600]};
||||||| 952f3cc3118
const SettingText = styled(Typography.Text)`
    font-size: 14px;
=======
const DescriptionText = styled.div`
    color: ${colors.gray[1700]};
    font-size: 14px;
    font-weight: 400;
    line-height: 1.5;
>>>>>>> master
`;

export const Preferences = () => {
    // Current User Urn
    const { user, refetchUser } = useUserContext();
    const isThemeV2 = useIsThemeV2();
    const [isThemeV2Toggleable] = useIsThemeV2Toggleable();
    const [isThemeV2EnabledForUser] = useIsThemeV2EnabledForUser();
    const showSimplifiedHomepage = !!user?.settings?.appearance?.showSimplifiedHomepage;

    const [updateUserSettingMutation] = useUpdateUserSettingMutation();

    const showSimplifiedHomepageSetting = !isThemeV2;
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const authenticatedUser = useGetAuthenticatedUser();
    const canManageOrganizationDisplayPreferences =
        authenticatedUser?.platformPrivileges?.manageOrganizationDisplayPreferences;

    return (
        <Page>
            <SourceContainer>
                <TokensContainer>
<<<<<<< HEAD
                    <PageTitle title="Appearance" subTitle="Manage your appearance settings." />
||||||| 952f3cc3118
                    <TokensHeaderContainer>
                        <TokensTitle level={2}>Appearance</TokensTitle>
                        <Typography.Paragraph type="secondary">Manage your appearance settings.</Typography.Paragraph>
                    </TokensHeaderContainer>
=======
                    <HeaderContainer>
                        <PageTitle title="Appearance" subTitle="Manage your appearance settings." />
                    </HeaderContainer>
>>>>>>> master
                </TokensContainer>
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
                {isThemeV2Toggleable && (
                    <>
                        <StyledCard>
                            <UserSettingRow>
<<<<<<< HEAD
                                <span>
                                    <SettingText>Try Acryl 2.0 (beta)</SettingText>
                                    <div>
                                        <DescriptionText>
                                            Enable an early preview of Acryl 2.0 - a complete makeover for your app with
                                            a sleek new design and advanced features. Flip the switch and refresh your
                                            browser to try it out!
                                        </DescriptionText>
                                    </div>
                                </span>
||||||| 952f3cc3118
                                <span>
                                    <SettingText>Try New User Experience</SettingText>
                                    <div>
                                        <DescriptionText>
                                            Enable an early preview of the new DataHub UX - a complete makeover for your
                                            app with a sleek new design and advanced features. Flip the switch and
                                            refresh your browser to try it out!
                                        </DescriptionText>
                                    </div>
                                </span>
=======
                                <TextContainer>
                                    <SettingText>Try New User Experience</SettingText>
                                    <DescriptionText>
                                        Enable an early preview of the new DataHub UX - a complete makeover for your app
                                        with a sleek new design and advanced features.
                                    </DescriptionText>
                                </TextContainer>
>>>>>>> master
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
<<<<<<< HEAD
                {canManageOrganizationDisplayPreferences && isShowNavBarRedesign && <OrganizationInfo />}
                {!showSimplifiedHomepageSetting && !isThemeV2Toggleable && !canManageOrganizationDisplayPreferences && (
                    <div style={{ color: ANTD_GRAY[7] }}>No appearance settings found.</div>
||||||| 952f3cc3118
                {!showSimplifiedHomepageSetting && !isThemeV2Toggleable && (
                    <div style={{ color: ANTD_GRAY[7] }}>No appearance settings found.</div>
=======
                {!showSimplifiedHomepageSetting && !isThemeV2Toggleable && (
                    <div style={{ color: colors.gray[1700] }}>No appearance settings found.</div>
>>>>>>> master
                )}
            </SourceContainer>
        </Page>
    );
};
