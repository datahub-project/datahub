import React, { useContext } from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { Skeleton } from 'antd';

import { NavLinks } from './NavLinks';
import { useAppConfig } from '../../useAppConfig';
import { useUserContext } from '../../context/useUserContext';
import { useEntityRegistry } from '../../useEntityRegistry';
import { EntityType } from '../../../types.generated';

import CustomAvatar from '../../shared/avatar/CustomAvatar';
import AcrylIcon from '../../../images/acryl-light-mark.svg?react';
import OnboardingContext from '../../onboarding/OnboardingContext';

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
    background-color: #3b2d94;
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
    border: 1px solid #32267d;
    background: #4c39be;
    box-shadow: 0px 4px 4px 0px rgba(0, 0, 0, 0.25);
    margin-bottom: 10px;

    & svg {
        height: 22px;
    }
`;

const CustomLogo = styled.img`
    object-fit: contain;
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

const SkeletonButton = styled(Skeleton.Button)`
    &&& {
        margin-top: 3px;
        width: 44px;
        height: 44px;
        margin-bottom: 10px;
    }
`;

const DEFAULT_LOGO = '/assets/logos/acryl-dark-mark.svg';

const NavSkeleton = () => {
    return (
        <>
            <SkeletonButton active shape="circle" />
            <SkeletonButton active shape="circle" style={{ height: '380px', borderRadius: '48px' }} />
            <Spacer />
            <UserIcon>
                <SkeletonButton active shape="circle" />
            </UserIcon>
        </>
    );
};

export const NavSidebar = () => {
    const appConfig = useAppConfig();
    const entityRegistry = useEntityRegistry();
    const userContext = useUserContext();
    const { urn, user } = userContext;
    const { isUserInitializing } = useContext(OnboardingContext);

    const customLogoUrl = appConfig.config.visualConfig.logoUrl;
    const hasCustomLogo = customLogoUrl && customLogoUrl !== DEFAULT_LOGO;

    const logoComponent = hasCustomLogo ? <CustomLogo alt="logo" src={customLogoUrl} /> : <AcrylIcon />;

    return (
        <Container>
            <Content>
                {isUserInitializing || !appConfig.loaded || !userContext.loaded ? (
                    <NavSkeleton />
                ) : (
                    <>
                        <Link to="/">
                            <Icon>{appConfig.loaded ? logoComponent : undefined}</Icon>
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
                    </>
                )}
            </Content>
        </Container>
    );
};
