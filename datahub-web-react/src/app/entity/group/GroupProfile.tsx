import { Alert } from 'antd';
import React from 'react';
import GroupHeader from './GroupHeader';
import { useGetGroupQuery } from '../../../graphql/group.generated';
import useUserParams from '../../shared/entitySearch/routingUtils/useUserParams';
import { EntityRelationshipsResult, EntityType } from '../../../types.generated';
import { Message } from '../../shared/Message';
import GroupMembers from './GroupMembers';
import { LegacyEntityProfile } from '../../shared/LegacyEntityProfile';
import { useEntityRegistry } from '../../useEntityRegistry';
import { decodeUrn } from '../shared/utils';
import GroupOwnerships from './GroupOwnerships';

const messageStyle = { marginTop: '10%' };

export enum TabType {
    Members = 'Members',
    Ownership = 'Ownership',
}

const ENABLED_TAB_TYPES = [TabType.Members, TabType.Ownership];

const MEMBER_PAGE_SIZE = 20;
const OWNERSHIP_PAGE_SIZE = 10;

/**
 * Responsible for reading & writing groups.
 */
export default function GroupProfile() {
    const entityRegistry = useEntityRegistry();
    const { urn: encodedUrn } = useUserParams();
    const urn = encodedUrn && decodeUrn(encodedUrn);
    const { loading, error, data } = useGetGroupQuery({ variables: { urn, membersCount: MEMBER_PAGE_SIZE } });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Group failed to load :('} />;
    }

    const groupMemberRelationships = data?.corpGroup?.relationships as EntityRelationshipsResult;

    const getTabs = () => {
        return [
            {
                name: TabType.Members,
                path: TabType.Members.toLocaleLowerCase(),
                content: (
                    <GroupMembers
                        urn={urn}
                        initialRelationships={groupMemberRelationships}
                        pageSize={MEMBER_PAGE_SIZE}
                    />
                ),
            },
            {
                name: TabType.Ownership,
                path: TabType.Ownership.toLocaleLowerCase(),
                content: <GroupOwnerships data={data} pageSize={OWNERSHIP_PAGE_SIZE} />,
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    const description = data?.corpGroup?.info?.description;

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={messageStyle} />}
            {data && data?.corpGroup && (
                <LegacyEntityProfile
                    title=""
                    tags={null}
                    header={
                        <GroupHeader
                            name={entityRegistry.getDisplayName(EntityType.CorpGroup, data?.corpGroup)}
                            description={description}
                            email={data?.corpGroup?.info?.email}
                        />
                    }
                    tabs={getTabs()}
                />
            )}
        </>
    );
}
