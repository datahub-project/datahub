import React from 'react';
import Cookies from 'js-cookie';
import { Dropdown } from 'antd';
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
import { MenuItemStyle } from '../entity/view/menu/item/styledComponent';

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

    const themeConfigItems = themeConfig.content.menu.items.map((value) => {
        return {
            key: value.label,
            label: (
                <MenuItemStyle key={value.label}>
                    <a
                        href={value.path || ''}
                        target={value.shouldOpenInNewTab ? '_blank' : ''}
                        rel="noopener noreferrer"
                        tabIndex={0}
                    >
                        {value.label}
                    </a>
                </MenuItemStyle>
            ),
        };
    });

    const divider = {
        key: 'divider',
        type: 'divider',
    };

    const items = [
        version
            ? {
                  key: 'version',
                  label: (
                      <MenuItemStyle key="version" disabled style={{ color: '#8C8C8C' }}>
                          {version}
                      </MenuItemStyle>
                  ),
              }
            : null,
        divider,
        {
            key: 'profile',
            label: (
                <MenuItemStyle key="profile">
                    <a
                        href={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${_urn}`}
                        rel="noopener noreferrer"
                        tabIndex={0}
                    >
                        Your Profile
                    </a>
                </MenuItemStyle>
            ),
        },
        ...themeConfigItems,
        {
            key: 'graphiQLLink',
            label: (
                <MenuItemStyle key="graphiQLLink">
                    <a href="/api/graphiql">GraphiQL</a>
                </MenuItemStyle>
            ),
        },
        {
            key: 'openapiLink',
            label: (
                <MenuItemStyle key="openapiLink">
                    <a href="/openapi/swagger-ui/index.html">OpenAPI</a>
                </MenuItemStyle>
            ),
        },
        divider,
        {
            key: 'logout',
            label: (
                <MenuItemStyle danger key="logout" tabIndex={0}>
                    <a href="/logOut" onClick={handleLogout} data-testid="log-out-menu-item">
                        Sign Out
                    </a>
                </MenuItemStyle>
            ),
        },
    ];

    return (
        <Dropdown menu={{ items }} trigger={['click']}>
            <DropdownWrapper data-testid="manage-account-menu">
                <CustomAvatar photoUrl={_pictureLink} style={{ marginRight: 4 }} name={name} />
                <DownArrow />
            </DropdownWrapper>
        </Dropdown>
    );
};

ManageAccount.defaultProps = defaultProps;
