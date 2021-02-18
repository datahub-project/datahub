import React from 'react';
import Cookies from 'js-cookie';
import { Menu, Avatar, Dropdown } from 'antd';
import { Link } from 'react-router-dom';
import defaultAvatar from '../../images/default_avatar.png';
import { EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import { isLoggedInVar } from '../auth/checkAuthStatus';

interface Props {
    urn: string;
    pictureLink?: string;
}

const defaultProps = {
    pictureLink: undefined,
};

export const ManageAccount = ({ urn: _urn, pictureLink: _pictureLink }: Props) => {
    const entityRegistry = useEntityRegistry();

    const handleLogout = () => {
        Cookies.remove('IS_LOGGED_IN');
        localStorage.removeItem('userUrn');
        isLoggedInVar(false);
    };

    const menu = (
        <Menu>
            <Menu.Item danger>
                <div tabIndex={0} role="button" onClick={handleLogout} onKeyDown={handleLogout}>
                    Log out
                </div>
            </Menu.Item>
        </Menu>
    );

    return (
        <Dropdown overlay={menu}>
            <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${_urn}`}>
                <Avatar
                    style={{
                        marginRight: '15px',
                        color: '#f56a00',
                        backgroundColor: '#fde3cf',
                    }}
                    src={_pictureLink || defaultAvatar}
                />
            </Link>
        </Dropdown>
    );
};

ManageAccount.defaultProps = defaultProps;
