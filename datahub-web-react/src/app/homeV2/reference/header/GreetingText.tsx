import React from 'react';
import styled from 'styled-components';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityType } from '../../../../types.generated';
import { useUserContext } from '../../../context/useUserContext';
import { getGreetingText } from './getGreetingText';

const Text = styled.div`
    font-size: 20px;
    padding: 0 17px 19px 17px;
    display: flex;
    flex-direction: column;
    align-items: start;
    justify-content: end;
    width: auto;
    overflow: hidden;
`;

const Name = styled.div`
    width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const Role = styled.div`
    font-size: 12px;
`;

export const GreetingText = ({ role }: { role?: string | null }) => {
    const entityRegistry = useEntityRegistry();
    const userContext = useUserContext();
    const greetingText = getGreetingText();
    const { user } = userContext;
    return (
        <Text>
            {!!user && (
                <>
                    {greetingText},<Name>{entityRegistry.getDisplayName(EntityType.CorpUser, user)}!</Name>
                    {(role && <Role>{role}</Role>) || null}
                </>
            )}{' '}
        </Text>
    );
};
