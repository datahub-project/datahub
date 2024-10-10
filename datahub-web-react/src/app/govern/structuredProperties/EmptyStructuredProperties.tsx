import { Text } from '@components';
import EmptyFormsImage from '@src/images/empty-forms.svg?react';
import React from 'react';
import { EmptyContainer } from './styledComponents';

const EmptyStructuredProperties = () => {
    return (
        <EmptyContainer>
            <EmptyFormsImage />
            <Text size="md" color="gray" weight="bold">
                No structured properties yet!
            </Text>
        </EmptyContainer>
    );
};

export default EmptyStructuredProperties;
