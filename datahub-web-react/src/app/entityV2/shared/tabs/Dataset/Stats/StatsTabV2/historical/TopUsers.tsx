import { Avatar, Table } from '@components';
import React, { useEffect } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import NoPermission from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/NoPermission';
import MoreInfoModalContent from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/MoreInfoModalContent';
import {
    SectionKeys,
    getMockTopUsersData,
    getUserOrGroupAvatarUrl,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { GraphCard } from '@src/alchemy-components/components/GraphCard';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import analytics, { EventType } from '@src/app/analytics';
import { HoverEntityTooltip } from '@src/app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Maybe, UserUsageCounts } from '@src/types.generated';

const CardWrapper = styled.div`
    display: flex;
    min-width: 30%;

    @media screen and (max-width: 2300px) {
        // align with highlights cards (1255 + 8px gap)
        width: calc(100% - 1233px);
    }
`;

interface Props {
    users?: Array<Maybe<UserUsageCounts>>;
}

const TopUsers = ({ users }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const {
        permissions: { canViewDatasetUsage },
        sections,
        setSectionState,
    } = useStatsSectionsContext();
    const isUsersDataPresent = canViewDatasetUsage && users && users?.length > 0;

    useEffect(() => {
        const currentSection = sections.users;
        const hasData = isUsersDataPresent || false;
        const loading = false;

        if (currentSection.hasData !== hasData || currentSection.isLoading !== loading) {
            setSectionState(SectionKeys.USERS, hasData, loading);
        }
    }, [isUsersDataPresent, sections.users, setSectionState]);

    const getUserOrGroupName = (user) => {
        return entityRegistry.getDisplayName(user.type, user);
    };

    function sendAnalytics() {
        analytics.event({ type: EventType.ClickUserProfile, location: 'statsTabTopUsers' });
    }

    const topUsersTableColumns = [
        {
            title: 'Name',
            key: 'name',
            render: (record) => {
                const userEntity = record.user;
                const avatarUrl = getUserOrGroupAvatarUrl(userEntity) || undefined;

                return (
                    <HoverEntityTooltip entity={userEntity} showArrow={false}>
                        <Link
                            to={`${entityRegistry.getEntityUrl(userEntity.type, userEntity.urn)}`}
                            onClick={sendAnalytics}
                        >
                            <Avatar name={getUserOrGroupName(userEntity)} imageUrl={avatarUrl} size="md" showInPill />
                        </Link>
                    </HoverEntityTooltip>
                );
            },
            sorter: (sourceA, sourceB) => {
                return getUserOrGroupName(sourceA.user).localeCompare(getUserOrGroupName(sourceB.user));
            },
            width: '50%',
        },
        {
            title: 'Queries last month',
            key: 'queries',
            alignment: 'right' as AlignmentOptions,
            render: (record) => {
                return record.count;
            },
            sorter: (sourceA, sourceB) => {
                return sourceA.count - sourceB.count;
            },
        },
    ];

    const renderTopUsersTable = () => (
        <Table columns={topUsersTableColumns} data={isUsersDataPresent ? users : getMockTopUsersData(4)} isScrollable />
    );

    return (
        <CardWrapper>
            <GraphCard
                title="Top Users"
                renderGraph={renderTopUsersTable}
                isEmpty={!isUsersDataPresent || !canViewDatasetUsage}
                emptyContent={!canViewDatasetUsage && <NoPermission statName="top users" />}
                moreInfoModalContent={<MoreInfoModalContent />}
            />
        </CardWrapper>
    );
};

export default TopUsers;
