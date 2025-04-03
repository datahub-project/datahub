import React from 'react';
import { Card, message } from 'antd';
import { Switch, PageTitle, colors } from '@components';
import styled from 'styled-components';
import { useUpdateUserSettingMutation } from '../../graphql/me.generated';
import { UserSetting } from '../../types.generated';
import analytics, { EventType } from '../analytics';
import { useUserContext } from '../context/useUserContext';
import { useIsThemeV2, useIsThemeV2EnabledForUser, useIsThemeV2Toggleable } from '../useIsThemeV2';

const Page = styled.div`
    width: 100%;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const HeaderContainer = styled.div`
    margin-bottom: 24px;
`;

const StyledCard = styled(Card)`
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px;
    display: flex;
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
    .ant-card-body {
        padding: 16px;
    }
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
`;

const UserSettingRow = styled.div`
    display: flex;
    justify-content: space-between;
    flex-direction: row;
`;

const SettingText = styled.div`
    font-size: 16px;
    color: ${colors.gray[600]};
    font-weight: 700;
`;

const DescriptionText = styled.div`
    color: ${colors.gray[1700]};
    font-size: 14px;
    font-weight: 400;
    line-height: 1.5;
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

    return (
        <Page>
            <SourceContainer>
                <TokensContainer>
                    <HeaderContainer>
                        <PageTitle title="Appearance" subTitle="Manage your appearance settings." />
                    </HeaderContainer>
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
                                <TextContainer>
                                    <SettingText>Try New User Experience</SettingText>
                                    <DescriptionText>
                                        Enable an early preview of the new DataHub UX - a complete makeover for your app
                                        with a sleek new design and advanced features. Flip the switch and refresh your
                                        browser to try it out!
                                    </DescriptionText>
                                </TextContainer>
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
                {!showSimplifiedHomepageSetting && !isThemeV2Toggleable && (
                    <div style={{ color: colors.gray[1700] }}>No appearance settings found.</div>
                )}
            </SourceContainer>
        </Page>
    );
};
