import React from 'react';
import Cookies from 'js-cookie';
import { Menu, Avatar, Dropdown } from 'antd';
import { CaretDownOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';
import defaultAvatar from '../../images/default_avatar.png';
import { EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import { GlobalCfg } from '../../conf';
import { isLoggedInVar } from '../auth/checkAuthStatus';

const MenuItem = styled(Menu.Item)`
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
const AvatarCircle = styled(Avatar)`
    color: #f56a00;
    margin-right: 5px;
    background-color: #fde3cf;
`;

const DownArrow = styled(CaretDownOutlined)`
    font-size: 18px;
    color: #fff;
`;

interface Props {
    urn: string;
    pictureLink?: string;
}

const defaultProps = {
    pictureLink: undefined,
};

export const ManageAccount = ({ urn: _urn, pictureLink: _pictureLink }: Props) => {
    const entityRegistry = useEntityRegistry();
    const themeConfig = useTheme();
    const handleLogout = () => {
        isLoggedInVar(false);
        Cookies.remove(GlobalCfg.CLIENT_AUTH_COOKIE);
    };

    const menu = (
        <Menu>
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
            <MenuItem danger key="logout" onClick={handleLogout} tabIndex={0}>
                Log out
            </MenuItem>
        </Menu>
    );

    return (
        <Dropdown overlay={menu}>
            <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${_urn}`}>
                <AvatarCircle src={_pictureLink || defaultAvatar} />
                <DownArrow />
            </Link>
        </Dropdown>
    );
};

ManageAccount.defaultProps = defaultProps;
