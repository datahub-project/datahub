/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import { Link } from 'react-router-dom';

import useGetUserName from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useGetUserName';
import { Avatar } from '@src/alchemy-components';
import { HoverEntityTooltip } from '@src/app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { CorpUser } from '@src/types.generated';

type TimelineDotProps = {
    user?: CorpUser;
};

export default function TimelineDot({ user }: TimelineDotProps) {
    const entityRegistry = useEntityRegistryV2();
    const getUserName = useGetUserName();

    const avatarUrl = user?.editableProperties?.pictureLink || undefined;

    if (!user) return null;

    return (
        <HoverEntityTooltip entity={user} showArrow={false}>
            <Link to={`${entityRegistry.getEntityUrl(user.type, user.urn)}`}>
                <Avatar name={getUserName(user)} imageUrl={avatarUrl} size="xl" />
            </Link>
        </HoverEntityTooltip>
    );
}
