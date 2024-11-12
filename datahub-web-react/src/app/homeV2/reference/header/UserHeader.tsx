import React, { useContext } from 'react';
import styled from 'styled-components';
import { Skeleton } from 'antd';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { useUserContext } from '../../../context/useUserContext';
import { GreetingText } from './GreetingText';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityType } from '../../../../types.generated';
import { UserHeaderImage } from './UserHeaderImage';
import { useUserPersonaTitle } from '../../persona/useUserPersona';
import OnboardingContext from '../../../onboarding/OnboardingContext';

const Container = styled.div`
    min-height: 240px;
    position: relative;
    display: flex;
`;

const GreetingTextWrapper = styled.div`
    color: #ffffff;
    position: absolute;
    background: linear-gradient(180deg, rgba(0, 0, 0, 0) 7%, #000 88.79%);
    width: 100%;
    height: 100%;
    display: flex;
    opacity: 0.8;
`;

const SkeletonButton = styled(Skeleton.Button)<{ $isShowNavBarRedesign?: boolean }>`
    &&& {
        width: 100%;
        min-height: 240px;
        border-top-left-radius: ${(props) =>
            props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '16px'};
        border-top-right-radius: ${(props) =>
            props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '16px'};
    }
`;

export const UserHeader = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const entityRegistry = useEntityRegistry();
    const { user } = useUserContext();
    const photoUrl = user?.editableProperties?.pictureLink || undefined;
    const displayName = user && entityRegistry.getDisplayName(EntityType.CorpUser, user);
    const maybeRole = useUserPersonaTitle();
    const { isUserInitializing } = useContext(OnboardingContext);

    return (
        <Container>
            {isUserInitializing || !user ? (
                <SkeletonButton shape="square" size="large" active block $isShowNavBarRedesign={isShowNavBarRedesign} />
            ) : (
                <>
                    <UserHeaderImage photoUrl={photoUrl} displayName={displayName || undefined} />
                    <GreetingTextWrapper>
                        <GreetingText role={maybeRole} />
                    </GreetingTextWrapper>
                </>
            )}
        </Container>
    );
};
