import { Card, Divider, Switch, Typography, message } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';

import { useUpdateUserSettingMutation } from '@graphql/me.generated';
import { UserSetting } from '@types';

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
    color: ${(props) => props.theme.colors.textTertiary};
    font-size: 11px;
`;

const SettingText = styled(Typography.Text)`
    font-size: 14px;
`;

export const Preferences = () => {
    const theme = useTheme();
    // Current User Urn
    const { user, refetchUser } = useUserContext();
    const showSimplifiedHomepage = !!user?.settings?.appearance?.showSimplifiedHomepage;

    const [updateUserSettingMutation] = useUpdateUserSettingMutation();

    const showSimplifiedHomepageSetting = false;

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
                {!showSimplifiedHomepageSetting && (
                    <div style={{ color: theme.colors.textTertiary }}>No appearance settings found.</div>
                )}
            </SourceContainer>
        </Page>
    );
};
