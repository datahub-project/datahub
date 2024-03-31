import React from 'react';
import styled from 'styled-components';
import { useUserContext } from '../../../context/useUserContext';
import { GreetingText } from './GreetingText';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityType } from '../../../../types.generated';
import { UserHeaderImage } from './UserHeaderImage';
import { useUserPersonaTitle } from '../../persona/useUserPersona';

const Container = styled.div`
    min-height: 240px;
    position: relative;
    display: flex;
`;

const GreetingTextWrapper = styled.div`
    color: #ffffff;
    position: absolute;
    background: linear-gradient(180deg, rgba(0, 0, 0, 0.00) 7%, #000 88.79%);
    width: 100%;
    height: 100%;
    display: flex;
    opacity: 0.8;
`;

export const UserHeader = () => {
    const entityRegistry = useEntityRegistry();
    const { user } = useUserContext();
    const photoUrl = user?.editableProperties?.pictureLink || undefined;
    const displayName = user && entityRegistry.getDisplayName(EntityType.CorpUser, user);
    const maybeRole = useUserPersonaTitle();
    return (
        <Container>
            <UserHeaderImage photoUrl={photoUrl} displayName={displayName || undefined} />
            <GreetingTextWrapper>
                <GreetingText role={maybeRole} />
            </GreetingTextWrapper>
        </Container>
    );
};
