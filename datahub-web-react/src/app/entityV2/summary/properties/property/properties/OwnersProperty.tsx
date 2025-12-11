/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Avatar } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { OwnerType } from '@types';

export default function OwnersProperty(props: PropertyComponentProps) {
    const entityRegistry = useEntityRegistryV2();

    const { entityData, loading } = useEntityContext();
    const owners = entityData?.ownership?.owners?.map((owner) => owner.owner) ?? [];

    const renderOwner = (owner: OwnerType) => {
        const displayName = entityRegistry.getDisplayName(owner.type, owner);
        const avatarUrl = owner.editableProperties?.pictureLink;

        return (
            <HoverEntityTooltip entity={owner} showArrow={false}>
                <Link to={`${entityRegistry.getEntityUrl(owner.type, owner.urn)}`} data-testid={`owner-${owner.urn}`}>
                    <Avatar name={displayName} imageUrl={avatarUrl} size="sm" showInPill />
                </Link>
            </HoverEntityTooltip>
        );
    };

    return (
        <BaseProperty
            {...props}
            values={owners}
            renderValue={renderOwner}
            restItemsPillBorderType="rounded"
            loading={loading}
        />
    );
}
