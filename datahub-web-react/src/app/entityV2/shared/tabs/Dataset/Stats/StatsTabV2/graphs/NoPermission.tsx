import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { EmptyMessageContainer } from '@src/alchemy-components/components/GraphCard/components';

const EmptyMessageWrapper = styled.div`
    text-align: center;
`;

interface Props {
    statName: string;
}

const NoPermission = ({ statName }: Props) => {
    return (
        <EmptyMessageContainer>
            <EmptyMessageWrapper>
                <Text size="2xl" weight="bold" color="gray">
                    No Permission
                </Text>
                <Text color="gray">{`You do not have permission to view ${statName} data.`}</Text>
            </EmptyMessageWrapper>
        </EmptyMessageContainer>
    );
};

export default NoPermission;
