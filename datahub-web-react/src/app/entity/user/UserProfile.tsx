import { Divider, Alert } from 'antd';
import React from 'react';
import styled from 'styled-components';

import UserHeader from './UserHeader';
import UserDetails from './UserDetails';
import useUserParams from './routingUtils/useUserParams';
import { useGetUserQuery } from '../../../graphql/user.generated';

const PageContainer = styled.div`
    background-color: white;
    padding: 32px 100px;
`;

/**
 * Responsible for reading & writing users.
 */
export default function UserProfile() {
    const { urn, subview, item } = useUserParams();
    const { loading, error, data } = useGetUserQuery({ variables: { urn } });

    if (loading) {
        return <Alert type="info" message="Loading" />;
    }

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    return (
        <PageContainer>
            <UserHeader
                profileSrc={data?.corpUser?.editableInfo?.pictureLink}
                name={data?.corpUser?.info?.displayName}
                title={data?.corpUser?.info?.title}
                email={data?.corpUser?.info?.email}
                skills={data?.corpUser?.editableInfo?.skills}
                teams={data?.corpUser?.editableInfo?.teams}
            />
            <Divider />
            <UserDetails urn={urn} subview={subview} item={item} ownerships={{}} />
        </PageContainer>
    );
}
