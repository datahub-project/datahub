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
        <EmptyMessageContainer data-testid="no-permissions">
            <EmptyMessageWrapper>
                <Text size="2xl" weight="bold">
                    No Permission
                </Text>
                <Text>{`You do not have permission to view ${statName} data.`}</Text>
            </EmptyMessageWrapper>
        </EmptyMessageContainer>
    );
};

export default NoPermission;
