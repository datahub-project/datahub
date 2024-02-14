import React from 'react';
import styled from 'styled-components';
import { useUserContext } from '../../../context/useUserContext';
import { GreetingText } from './GreetingText';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityType } from '../../../../types.generated';
import { UserHeaderImage } from './UserHeaderImage';
import { useUserPersonaTitle } from '../../persona/useUserPersona';

const Container = styled.div``;

const GreetingTextWrapper = styled.div`
    margin-top: -114px;
    max-height: 80px;
    width: auto;
    margin-left: 12px;
    color: #ffffff;
    margin-bottom: 40px;
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
