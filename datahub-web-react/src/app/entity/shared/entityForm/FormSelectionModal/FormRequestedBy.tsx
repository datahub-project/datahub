/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
