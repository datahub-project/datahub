import React from 'react';
import EmptyFormsImage from '@src/images/empty-forms.svg?react';
import { EmptyContainer } from '@src/app/govern/Dashboard/Forms/styledComponents';
import { Text } from '@src/alchemy-components';

const EmptyProposals = () => {
    return (
        <EmptyContainer>
            <EmptyFormsImage />
            <Text size="md" color="gray" weight="bold">
                No tasks available!
            </Text>
        </EmptyContainer>
    );
};

export default EmptyProposals;
