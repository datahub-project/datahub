import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { EmptyContainer } from '@app/govern/structuredProperties/styledComponents';
import EmptyFormsImage from '@src/images/empty-forms.svg?react';

export const TextContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
`;

interface Props {
    sourceType?: string;
    isEmptySearchResult?: boolean;
}

const EmptySources = ({ sourceType, isEmptySearchResult }: Props) => {
    return (
        <EmptyContainer>
            {isEmptySearchResult ? (
                <TextContainer>
                    <Text size="lg" weight="bold">
                        No search results!
                    </Text>
                    <Text size="sm" weight="normal">
                        Try another search query with at least 3 characters...
                    </Text>
                </TextContainer>
            ) : (
                <>
                    <EmptyFormsImage />
                    <Text size="md" weight="bold">
                        {`No ${sourceType || 'sources'} yet!`}
                    </Text>
                </>
            )}
        </EmptyContainer>
    );
};

export default EmptySources;
