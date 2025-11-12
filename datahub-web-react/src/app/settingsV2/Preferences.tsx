import { PageTitle, Switch, colors } from '@components';
import { message } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
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

const DescriptionText = styled.div`
    color: ${colors.gray[1700]};
    font-size: 14px;
    font-weight: 400;
    line-height: 1.5;
`;

export const Preferences = () => {
    const userContext = useUserContext();
    const appConfig = useAppConfig();

    const applicationsEnabled = appConfig.config?.visualConfig?.application?.showApplicationInNavigation ?? false;

    const [updateApplicationsSettingsMutation] = useUpdateApplicationsSettingsMutation();

    const canManageApplicationAppearance = userContext?.platformPrivileges?.manageFeatures;

    return (
        <Page>
            <SourceContainer>
                <TokensContainer>
                    <HeaderContainer>
                        <PageTitle title="Appearance" subTitle="Manage your appearance settings." />
                    </HeaderContainer>
                </TokensContainer>
                {canManageApplicationAppearance && (
                    <StyledCard>
                        <UserSettingRow>
                            <TextContainer>
                                <SettingText>Show Applications</SettingText>
                                <DescriptionText>
                                    Applications are another way to organize your data, similar to Domains. They are
                                    hidden by default.
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
                {!canManageApplicationAppearance && (
                    <div style={{ color: colors.gray[1700] }}>No appearance settings found.</div>
                )}
            </SourceContainer>
        </Page>
    );
};
