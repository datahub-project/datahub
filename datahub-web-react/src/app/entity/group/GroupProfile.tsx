import { Alert } from 'antd';
import React, { useMemo } from 'react';
import GroupHeader from './GroupHeader';
import { useGetUserGroupQuery } from '../../../graphql/user.generated';
import { useGetAllEntitySearchResults } from '../../../utils/customGraphQL/useGetAllEntitySearchResults';
import useUserParams from '../../shared/entitySearch/routingUtils/useUserParams';
import { EntityProfile } from '../../shared/EntityProfile';
import { CorpUser, EntityType, SearchResult } from '../../../types.generated';
import RelatedEntityResults from '../../shared/entitySearch/RelatedEntityResults';
import { Message } from '../../shared/Message';
import GroupMembers from './GroupMembers';

const messageStyle = { marginTop: '10%' };

export enum TabType {
    Members = 'Members',
    Ownership = 'Ownership',
}

const ENABLED_TAB_TYPES = [TabType.Members, TabType.Ownership];

/**
 * Responsible for reading & writing users.
 */
export default function GroupProfile() {
    const { urn } = useUserParams();
    const { loading, error, data } = useGetUserGroupQuery({ variables: { urn } });

    const name = data?.corpGroup?.name;

    const ownershipResult = useGetAllEntitySearchResults({
        query: `owners:${name}`,
    });

    const contentLoading =
        Object.keys(ownershipResult).some((type) => {
            return ownershipResult[type].loading;
        }) || loading;

    const ownershipForDetails = useMemo(() => {
        const filteredOwnershipResult: {
            [key in EntityType]?: Array<SearchResult>;
        } = {};

        Object.keys(ownershipResult).forEach((type) => {
            const entities = ownershipResult[type].data?.search?.searchResults;

            if (entities && entities.length > 0) {
                filteredOwnershipResult[type] = ownershipResult[type].data?.search?.searchResults;
            }
        });
        return filteredOwnershipResult;
    }, [ownershipResult]);

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const getTabs = () => {
        return [
            {
                name: TabType.Members,
                path: TabType.Members.toLocaleLowerCase(),
                content: (
                    <GroupMembers members={data?.corpGroup?.relationships?.map((rel) => rel?.entity as CorpUser)} />
                ),
            },
            {
                name: TabType.Ownership,
                path: TabType.Ownership.toLocaleLowerCase(),
                content: <RelatedEntityResults searchResult={ownershipForDetails} />,
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    const description = data?.corpGroup?.info?.description;

    return (
        <>
            {contentLoading && <Message type="loading" content="Loading..." style={messageStyle} />}
            {data && data?.corpGroup && (
                <EntityProfile
                    title=""
                    tags={null}
                    header={
                        <GroupHeader
                            name={data?.corpGroup?.name}
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
