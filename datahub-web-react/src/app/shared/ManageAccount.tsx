import React from 'react';
import Cookies from 'js-cookie';
import { Menu, Dropdown } from 'antd';
import { CaretDownOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import { GlobalCfg } from '../../conf';
import { isLoggedInVar } from '../auth/checkAuthStatus';
import CustomAvatar from './avatar/CustomAvatar';
import analytics, { EventType } from '../analytics';
import { ANTD_GRAY } from '../entity/shared/constants';
import { useUserContext } from '../context/useUserContext';
import { MenuItem } from './admin/components';

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
    const userContext = useUserContext();
    const handleLogout = () => {
        analytics.event({ type: EventType.LogOutEvent });
        isLoggedInVar(false);
        Cookies.remove(GlobalCfg.CLIENT_AUTH_COOKIE);
        userContext.updateLocalState({ selectedViewUrn: undefined });
    };
    const menu = (
        <Menu style={{ width: '120px' }}>
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
            <MenuItem danger key="logout" tabIndex={0}>
                <a href="/logOut" onClick={handleLogout} data-testid="log-out-menu-item">
                    Sign Out
                </a>
            </MenuItem>
        </Menu>
    );

    return (
        <Dropdown overlay={menu} trigger={['click']}>
            <DropdownWrapper data-testid="manage-account-menu">
                <CustomAvatar photoUrl={_pictureLink} style={{ marginRight: 4 }} name={name} />
                <DownArrow />
            </DropdownWrapper>
        </Dropdown>
    );
};

ManageAccount.defaultProps = defaultProps;
