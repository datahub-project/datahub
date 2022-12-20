import React from 'react';
import Cookies from 'js-cookie';
import { Menu, Dropdown } from 'antd';
import { CaretDownOutlined } from '@ant-design/icons';
import styled, { useTheme } from 'styled-components';
import { EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import { GlobalCfg } from '../../conf';
import { isLoggedInVar } from '../auth/checkAuthStatus';
import CustomAvatar from './avatar/CustomAvatar';
import analytics, { EventType } from '../analytics';
import { ANTD_GRAY } from '../entity/shared/constants';
import { useAppConfig } from '../useAppConfig';
import { useUserContext } from '../context/useUserContext';

const MenuItem = styled(Menu.Item)`
    display: flex;
    justify-content: start;
    align-items: center;
    && {
        margin-top: 2px;
    }
    & > a:visited,
    & > a:active,
    & > a:focus {
        clear: both;
        border: none;
        outline: 0;
    }
`;

const DownArrow = styled(CaretDownOutlined)`
    vertical-align: -1px;
    font-size: 10px;
    color: ${ANTD_GRAY[7]};
`;

const DropdownWrapper = styled.div`
    align-items: center;
    cursor: pointer;
    display: flex;
`;

interface Props {
    urn: string;
    pictureLink?: string;
    name?: string;
}

const defaultProps = {
    pictureLink: undefined,
};

export const ManageAccount = ({ urn: _urn, pictureLink: _pictureLink, name }: Props) => {
    const entityRegistry = useEntityRegistry();
    const themeConfig = useTheme();
    const { config } = useAppConfig();
    const userContext = useUserContext();
    const handleLogout = () => {
        analytics.event({ type: EventType.LogOutEvent });
        isLoggedInVar(false);
        Cookies.remove(GlobalCfg.CLIENT_AUTH_COOKIE);
        userContext.updateLocalState({ selectedViewUrn: undefined });
    };
    const version = config?.appVersion;
    const menu = (
        <Menu style={{ width: '120px' }}>
            {version && (
                <MenuItem key="version" disabled style={{ color: '#8C8C8C' }}>
                    {version}
                </MenuItem>
            )}
            <Menu.Divider />
            <MenuItem key="profile">
                <a
                    href={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${_urn}`}
                    rel="noopener noreferrer"
                    tabIndex={0}
                >
                    Your Profile
                </a>
            </MenuItem>
            <Menu.Divider />
            {themeConfig.content.menu.items.map((value) => {
                return (
                    <MenuItem key={value.label}>
                        <a
                            href={value.path || ''}
                            target={value.shouldOpenInNewTab ? '_blank' : ''}
                            rel="noopener noreferrer"
                            tabIndex={0}
                        >
                            {value.label}
                        </a>
                    </MenuItem>
                );
            })}
            <MenuItem key="graphiQLLink">
                <a href="/api/graphiql">GraphiQL</a>
            </MenuItem>
            <MenuItem key="openapiLink">
                <a href="/openapi/swagger-ui/index.html">OpenAPI</a>
            </MenuItem>
            <Menu.Divider />
            <MenuItem danger key="logout" tabIndex={0}>
                <a href="/logOut" onClick={handleLogout}>
                    Sign Out
                </a>
            </MenuItem>
        </Menu>
    );

    return (
        <Dropdown overlay={menu} trigger={['click']}>
            <DropdownWrapper>
                <CustomAvatar photoUrl={_pictureLink} style={{ marginRight: 4 }} name={name} />
                <DownArrow />
            </DropdownWrapper>
        </Dropdown>
    );
};

ManageAccount.defaultProps = defaultProps;
