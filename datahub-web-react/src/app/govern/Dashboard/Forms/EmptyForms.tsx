import { Text } from '@components';
import React from 'react';

import { EmptyContainer } from '@app/govern/Dashboard/Forms/styledComponents';
import EmptyFormsImage from '@src/images/empty-forms.svg?react';

interface Props {
    isEmptySearch?: boolean;
}

const EmptyForms = ({ isEmptySearch }: Props) => {
    return (
        <EmptyContainer>
            {isEmptySearch ? (
                <Text size="lg" color="gray" weight="bold">
                    No search results!
                </Text>
            ) : (
                <>
                    <EmptyFormsImage />
                    <Text size="md" color="gray" weight="bold">
                        No forms yet!
                    </Text>
                </>
            )}
        </EmptyContainer>
    );
};

export default EmptyForms;
