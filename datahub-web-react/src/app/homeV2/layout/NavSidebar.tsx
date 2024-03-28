import React from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';

import { NavLinks } from './NavLinks';
import { useAppConfig } from '../../useAppConfig';
import { DEFAULT_APP_CONFIG } from '../../../appConfigContext';
import { useUserContext } from '../../context/useUserContext';
import { useEntityRegistry } from '../../useEntityRegistry';
import { EntityType } from '../../../types.generated';

import CustomAvatar from '../../shared/avatar/CustomAvatar';
import AcrylIcon from '../../../images/acryl-light-mark.svg?react';

const Container = styled.div`
    height: 100vh;
    padding: 12px;
    background-color: none;
`;

const Content = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: space-between;
    background-color: #3B2D94;
    border-radius: 32px;
    height: 100%;
    width: 52px;
`;

const Icon = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    margin-top: 3px;
    width: 44px;
    height: 44px;
    border-radius: 38px;
    border: 1px solid #32267D;
    background: #4C39BE;
    box-shadow: 0px 4px 4px 0px rgba(0, 0, 0, 0.25);
    margin-bottom: 10px;

    & svg {
        height: 22px;
    }
`;

const CustomLogo = styled.img`
    max-height: 26px;
    max-width: 26px;
    min-height: 20px;
    min-width: 20px;
`;

const Spacer = styled.div`
    flex: 1;
`;

const UserIcon = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 44px;
    height: 44px;
    border-radius: 38px;
    overflow: hidden;
    margin-bottom: 3px;
    box-shadow: 0px 4px 4px 0px rgba(0, 0, 0, 0.25);

    .ant-avatar {
        margin: 0;
    }
`;

const DEFAULT_LOGO = "/assets/logos/acryl-dark-mark.svg";

export const NavSidebar = () => {
    const appConfig = useAppConfig();
    const entityRegistry = useEntityRegistry();
    const { urn, user } = useUserContext();

    const customLogoUrl = appConfig.config.visualConfig.logoUrl;
    const hasCustomLogo = customLogoUrl && customLogoUrl !== DEFAULT_LOGO;

    const logoComponent = hasCustomLogo ? <CustomLogo alt="logo" src={customLogoUrl} /> : <AcrylIcon />;

    return (
        <Container>
            <Content>
                <Link to="/">
                    <Icon>{appConfig.config !== DEFAULT_APP_CONFIG ? logoComponent : undefined}</Icon>
                </Link>
                <NavLinks />
                <Spacer />
                <UserIcon>
                    <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${urn}`}>
                        <CustomAvatar
                            photoUrl={user?.editableProperties?.pictureLink || undefined}
                            name={user?.editableProperties?.displayName || ''}
                            size={44}
                            hideTooltip
                        />
                    </Link>
                </UserIcon>
            </Content>
        </Container>
    );
};
