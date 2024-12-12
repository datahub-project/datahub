import { Avatar, Table } from '@components';
import { GraphCard } from '@src/alchemy-components/components/GraphCard';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { HoverEntityTooltip } from '@src/app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Maybe, UserUsageCounts } from '@src/types.generated';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { getMockTopUsersData } from './utils';

const CardWrapper = styled.div`
    display: flex;
    justify-self: end;
    width: 30%;
`;

interface Props {
    users?: Array<Maybe<UserUsageCounts>>;
}

const TopUsers = ({ users }: Props) => {
    const entityRegistry = useEntityRegistryV2();
    const isUsersDataPresent = users && users?.length > 0;

    const getUserOrGroupName = (user) => {
        return entityRegistry.getDisplayName(user.type, user);
    };

    const topUsersTableColumns = [
        {
            title: 'Name',
            key: 'name',
            render: (record) => {
                const userEntity = record.user;
                const avatarUrl = userEntity.editableProperties?.pictureLink || undefined;

                return (
                    <HoverEntityTooltip entity={userEntity} showArrow={false}>
                        <Link to={`${entityRegistry.getEntityUrl(userEntity.type, userEntity.urn)}`}>
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
            title: 'Queries per month',
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
            <GraphCard title="Top Users" renderGraph={renderTopUsersTable} isEmpty={!isUsersDataPresent} />
        </CardWrapper>
    );
};

export default TopUsers;
