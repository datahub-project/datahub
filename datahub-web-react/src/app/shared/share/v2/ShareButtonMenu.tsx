/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import CopyLinkMenuItem from '@app/shared/share/v2/items/CopyLinkMenuItem';
import CopyNameMenuItem from '@app/shared/share/v2/items/CopyNameMenuItem';
import CopyUrnMenuItem from '@app/shared/share/v2/items/CopyUrnMenuItem';
import EmailMenuItem from '@app/shared/share/v2/items/EmailMenuItem';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

interface ShareButtonMenuProps {
    urn: string;
    entityType: EntityType;
    subType?: string | null;
    name?: string | null;
    qualifiedName?: string | null;
}

export default function ShareButtonMenu({ urn, entityType, subType, name, qualifiedName }: ShareButtonMenuProps) {
    const entityRegistry = useEntityRegistry();

    const displayName = name || urn;
    const displayType = subType || entityRegistry.getEntityName(entityType) || entityType;

    return (
        <>
            {navigator.clipboard && <CopyLinkMenuItem key="0" urn={urn} entityType={entityType} />}

            {navigator.clipboard && <CopyUrnMenuItem key="1" urn={urn} type={displayType} />}

            {navigator.clipboard && (
                <CopyNameMenuItem
                    key="2"
                    name={displayName}
                    type={displayType}
                    qualifiedName={qualifiedName || undefined}
                />
            )}

            <EmailMenuItem key="4" urn={urn} name={displayName} type={displayType} />
        </>
    );
}
