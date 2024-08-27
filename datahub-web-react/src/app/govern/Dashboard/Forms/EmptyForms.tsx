import { Text } from '@components';
import EmptyFormsImage from '@src/images/empty-forms.svg?react';
import React from 'react';
import { EmptyContainer } from './styledComponents';

const EmptyForms = () => {
    return (
        <EmptyContainer>
            <EmptyFormsImage />
            <Text size="md" color="gray" weight="bold">
                No forms yet!
            </Text>
        </EmptyContainer>
    );
};

export default EmptyForms;
