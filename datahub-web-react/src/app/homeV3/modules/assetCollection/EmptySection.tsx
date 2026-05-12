import { Text } from '@components';
import React from 'react';

import { EmptyContainer } from '@app/homeV3/styledComponents';

const EmptySection = () => {
    return (
        <EmptyContainer>
            <Text>No assets found.</Text>
        </EmptyContainer>
    );
};

export default EmptySection;
