import { Card, Divider, message, Switch, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useUpdateUserSettingMutation } from '../../graphql/me.generated';
import { UserSetting } from '../../types.generated';
import analytics, { EventType } from '../analytics';
import { useUserContext } from '../context/useUserContext';
import { ANTD_GRAY } from '../entity/shared/constants';
import { useIsThemeV2, useIsThemeV2EnabledForUser, useIsThemeV2Toggleable } from '../useIsThemeV2';

const Page = styled.div`
    width: 100%;
    display: flex;
    justify-content: center;
`;

const SourceContainer = styled.div`
    width: 80%;
    padding-top: 20px;
    padding-right: 40px;
    padding-left: 40px;
`;

const TokensContainer = styled.div`
    padding-top: 0px;
`;

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

const UserSettingRow = styled.div`
    display: flex;
    justify-content: space-between;
`;

const DescriptionText = styled(Typography.Text)`
    color: ${ANTD_GRAY[7]};
    font-size: 11px;
`;

const SettingText = styled(Typography.Text)`
    font-size: 14px;
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
                    <TokensHeaderContainer>
                        <TokensTitle level={2}>Appearance</TokensTitle>
                        <Typography.Paragraph type="secondary">Manage your appearance settings.</Typography.Paragraph>
                    </TokensHeaderContainer>
                </TokensContainer>
                <Divider />
                {showSimplifiedHomepageSetting && (
                    <Card>
                        <UserSettingRow>
                            <span>
                                <SettingText>Show simplified homepage </SettingText>
                                <div>
                                    <DescriptionText>
                                        Limits entity browse cards on homepage to Domains, Charts, Datasets, Dashboards
                                        and Glossary Terms
                                    </DescriptionText>
                                </div>
                            </span>
                            <Switch
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
                    </Card>
                )}
                {isThemeV2Toggleable && (
                    <>
                        <Card style={{ marginTop: 20 }}>
                            <UserSettingRow>
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
                                <Switch
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
                        </Card>
                    </>
                )}
                {!showSimplifiedHomepageSetting && !isThemeV2Toggleable && (
                    <div style={{ color: ANTD_GRAY[7] }}>No appearance settings found.</div>
                )}
            </SourceContainer>
        </Page>
    );
};
