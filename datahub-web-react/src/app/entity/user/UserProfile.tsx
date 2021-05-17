import { Divider, Alert } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import UserHeader from './UserHeader';
import UserDetails from './UserDetails';
import useUserParams from './routingUtils/useUserParams';
import { useGetUserQuery } from '../../../graphql/user.generated';
import { useGetAllEntitySearchResults } from '../../../utils/customGraphQL/useGetAllEntitySearchResults';
import { Message } from '../../shared/Message';

const PageContainer = styled.div`
    padding: 32px 100px;
`;

const messageStyle = { marginTop: '10%' };

/**
 * Responsible for reading & writing users.
 */
export default function UserProfile() {
    const { urn, subview, item } = useUserParams();
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
        Object.keys(ownershipResult).forEach((type) => {
            const entities = ownershipResult[type].data?.search?.searchResults;

            if (!entities || entities.length === 0) {
                delete ownershipResult[type];
            } else {
                ownershipResult[type] = ownershipResult[type].data?.search?.searchResults;
            }
        });
        return ownershipResult;
    }, [ownershipResult]);

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    return (
        <PageContainer>
            {contentLoading && <Message type="loading" content="Loading..." style={messageStyle} />}
            <UserHeader
                profileSrc={data?.corpUser?.editableInfo?.pictureLink}
                name={data?.corpUser?.info?.displayName}
                title={data?.corpUser?.info?.title}
                email={data?.corpUser?.info?.email}
                skills={data?.corpUser?.editableInfo?.skills}
                teams={data?.corpUser?.editableInfo?.teams}
            />
            <Divider />
            <UserDetails urn={urn} subview={subview} item={item} ownerships={ownershipForDetails} />
        </PageContainer>
    );
}
