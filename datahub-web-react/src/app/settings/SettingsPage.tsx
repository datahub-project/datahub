import { Menu, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../entity/shared/constants';
import { SearchablePage } from '../search/SearchablePage';
import { AccessTokens } from './AccessTokens';

const PageContainer = styled.div`
    display: flex;
`;

const SettingsBarContainer = styled.div`
    padding-top: 20px;
    height: 100vh;
    border-right: 1px solid ${ANTD_GRAY[5]};
`;

const SettingsBarHeader = styled.div`
    && {
        padding-left: 24px;
    }
    margin-bottom: 20px;
`;

const PageTitle = styled(Typography.Title)`
    && {
        margin-bottom: 8px;
    }
`;

export const SettingsPage = () => {
    const [selectedKey, setSelectedKey] = useState('access-tokens');

    const onMenuClick = ({ key }) => {
        setSelectedKey(key);
    };

    return (
        <SearchablePage>
            <PageContainer>
                <SettingsBarContainer>
                    <SettingsBarHeader>
                        <PageTitle level={3}>Settings</PageTitle>
                        <Typography.Paragraph type="secondary">Manage your DataHub settings.</Typography.Paragraph>
                    </SettingsBarHeader>
                    <Menu
                        selectable={false}
                        mode="inline"
                        style={{ width: 256 }}
                        selectedKeys={[selectedKey]}
                        onClick={(key) => {
                            onMenuClick(key);
                        }}
                    >
                        <Menu.Item key="access-tokens">Access Tokens</Menu.Item>
                    </Menu>
                </SettingsBarContainer>
                {selectedKey === 'access-tokens' && <AccessTokens />}
            </PageContainer>
        </SearchablePage>
    );
};
