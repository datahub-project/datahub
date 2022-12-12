import React from 'react';
import styled from 'styled-components';
import { Maybe, Ownership } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import AvatarsGroup from '../shared/avatar/AvatarsGroup';

const AvatarGroupWrapper = styled.div`
    margin-right: 10px;
    display: inline-block;
`;

type Props = {
    ownership?: Maybe<Ownership>;
};

export default function DomainOwners({ ownership }: Props) {
    const entityRegistry = useEntityRegistry();

    if (!ownership) {
        return null;
    }

    const { owners } = ownership;
    if (!owners || owners.length === 0) {
        return null;
    }
    return (
        <AvatarGroupWrapper>
            <AvatarsGroup size={24} owners={owners} entityRegistry={entityRegistry} maxCount={4} />
        </AvatarGroupWrapper>
    );
}
