import React from 'react';

import { Text } from '@src/alchemy-components';
import { EmptyContainer } from '@src/app/govern/Dashboard/Forms/styledComponents';
import EmptyFormsImage from '@src/images/empty-forms.svg?react';

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
