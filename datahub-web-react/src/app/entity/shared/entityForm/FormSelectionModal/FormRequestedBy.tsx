import React from 'react';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { Owner } from '@types';

interface Props {
    owners: Owner[];
}

export default function FormRequestedBy({ owners }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <>
            Requested by:{' '}
            {owners.map((ownerAssoc, index) => (
                <>
                    {owners.length > 1 && index === owners.length - 1 && 'and '}
                    {entityRegistry.getDisplayName(ownerAssoc.owner.type, ownerAssoc.owner)}
                    {owners.length > 1 && index !== owners.length - 1 && ', '}
                </>
            ))}
        </>
    );
}
