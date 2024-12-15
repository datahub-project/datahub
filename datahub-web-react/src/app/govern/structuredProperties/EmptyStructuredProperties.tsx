import { Text } from '@components';
import EmptyFormsImage from '@src/images/empty-forms.svg?react';
import React from 'react';
import { EmptyContainer } from './styledComponents';

interface Props {
    isEmptySearch?: boolean;
}

const EmptyStructuredProperties = ({ isEmptySearch }: Props) => {
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
                        No properties yet!
                    </Text>
                </>
            )}
        </EmptyContainer>
    );
};

export default EmptyStructuredProperties;
