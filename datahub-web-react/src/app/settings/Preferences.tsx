import React from 'react';
import styled from 'styled-components';
import { Divider, Typography, Switch, Card, message } from 'antd';

import { useGetMeQuery, useUpdateUserSettingMutation } from '../../graphql/me.generated';
import { UserSetting } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';

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
    const { data, refetch } = useGetMeQuery({ fetchPolicy: 'no-cache' });

    const showSimplifiedHomepage = !!data?.me?.corpUser?.settings?.appearance?.showSimplifiedHomepage;
    const [updateUserSettingMutation] = useUpdateUserSettingMutation();

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
                <Card>
                    <UserSettingRow>
                        <span>
                            <SettingText>Show simplified hompeage </SettingText>
                            <div>
                                <DescriptionText>
                                    Limits entity browse cards on homepage to Domains, Charts, Datasets, Dashboards and
                                    Glossary Terms
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
                                message.success({ content: 'Setting updated!', duration: 2 });
                                refetch?.();
                            }}
                        />
                    </UserSettingRow>
                </Card>
            </SourceContainer>
        </Page>
    );
};
