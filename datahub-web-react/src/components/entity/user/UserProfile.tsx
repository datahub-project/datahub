import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { EntityType, PlatformNativeType } from '../../../types.generated';
import UserHeader from './UserHeader';
import UserDetails from './UserDetails';
import useUserParams from './routingUtils/useUserParams';

const PageContainer = styled.div`
    background-color: white;
    padding: 32px 100px;
`;

/**
 * Responsible for reading & writing users.
 */
export default function UserProfile() {
    const { urn, subview, item } = useUserParams();

    return (
        <PageContainer>
            <UserHeader
                name="Jane Doe"
                title="Software Engineer"
                skills={['Pandas', 'Multivariate Calculus', 'Juggling']}
                teams={['Product', 'Data Science']}
                email="jane@datahub.ui"
            />
            <Divider />
            <UserDetails
                urn={urn}
                subview={subview}
                item={item}
                ownerships={{
                    [EntityType.Dataset]: [
                        {
                            name: 'HiveDataset',
                            origin: 'PROD',
                            description: 'this is a dataset',
                            platformNativeType: PlatformNativeType.Table,
                        },
                        {
                            name: 'KafkaDataset',
                            origin: 'PROD',
                            description: 'this is also a dataset',
                            platformNativeType: PlatformNativeType.Table,
                        },
                    ],
                }}
            />
        </PageContainer>
    );
}
