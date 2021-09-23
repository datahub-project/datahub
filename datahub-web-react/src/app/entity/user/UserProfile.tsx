import { Alert } from 'antd';
import React, { useMemo } from 'react';

import UserHeader from './UserHeader';
import useUserParams from '../../shared/entitySearch/routingUtils/useUserParams';
import { useGetUserQuery } from '../../../graphql/user.generated';
import { useGetAllEntitySearchResults } from '../../../utils/customGraphQL/useGetAllEntitySearchResults';
import { Message } from '../../shared/Message';
import RelatedEntityResults from '../../shared/entitySearch/RelatedEntityResults';
import { LegacyEntityProfile } from '../../shared/LegacyEntityProfile';
import { CorpUser, EntityType, SearchResult } from '../../../types.generated';

const messageStyle = { marginTop: '10%' };

export enum TabType {
    Ownership = 'Ownership',
}
const ENABLED_TAB_TYPES = [TabType.Ownership];

/**
 * Responsible for reading & writing users.
 */
export default function UserProfile() {
    const { urn } = useUserParams();
    const { loading, error, data } = useGetUserQuery({ variables: { urn } });
    const username = data?.corpUser?.username;

    const ownershipResult = useGetAllEntitySearchResults({
        query: `owners:${username}`,
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
                name: TabType.Ownership,
                path: TabType.Ownership.toLocaleLowerCase(),
                content: <RelatedEntityResults searchResult={ownershipForDetails} />,
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    const getHeader = ({ editableInfo, info }: CorpUser) => {
        return (
            <UserHeader
                profileSrc={editableInfo?.pictureLink}
                name={info?.displayName}
                title={info?.title}
                email={info?.email}
                skills={editableInfo?.skills}
                teams={editableInfo?.teams}
            />
        );
    };

    return (
        <>
            {contentLoading && <Message type="loading" content="Loading..." style={messageStyle} />}
            {data && data.corpUser && (
                <LegacyEntityProfile
                    title=""
                    tags={null}
                    header={getHeader(data.corpUser as CorpUser)}
                    tabs={getTabs()}
                />
            )}
        </>
    );
}
