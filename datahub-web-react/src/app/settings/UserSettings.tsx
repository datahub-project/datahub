import React from 'react';
import styled from 'styled-components';
import { Divider, Typography, Switch, message } from 'antd';
import { useGetMeQuery, useUpdateUserSettingMutation } from '../../graphql/me.generated';
import { UserSetting } from '../../types.generated';

const SourceContainer = styled.div`
    width: 100%;
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

export const UserSettings = () => {
    // Current User Urn
    const { data, refetch } = useGetMeQuery({ fetchPolicy: 'no-cache' });

    const showSimplifiedHomepage = !!data?.me?.corpUser?.settings?.showSimplifiedHomepage;
    const [updateUserSettingMutation] = useUpdateUserSettingMutation();

    return (
        <SourceContainer>
            <TokensContainer>
                <TokensHeaderContainer>
                    <TokensTitle level={2}>User Settings</TokensTitle>
                    <Typography.Paragraph type="secondary">Manage your settings.</Typography.Paragraph>
                </TokensHeaderContainer>
            </TokensContainer>
            <Divider />
            <Typography.Text>Show simplified hompeage </Typography.Text>
            <Switch
                checked={showSimplifiedHomepage}
                onChange={async () => {
                    await updateUserSettingMutation({
                        variables: {
                            input: { name: UserSetting.ShowSimplifiedHomepage, value: !showSimplifiedHomepage },
                        },
                    });
                    message.success({ content: 'Setting updated!', duration: 2 });
                    refetch?.();
                }}
            />
        </SourceContainer>
    );
};
