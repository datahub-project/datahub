import React from 'react';
import { Button, Text } from '@src/alchemy-components';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 8px 0;
`;

const InlineButton = styled(Button)`
    display: inline;
    padding: 0px;
    background: none;

    &:hover {
        background: none;
    }
`;

interface Props {
    hasAppliedFilters?: boolean;
    onClearFilters?: () => void;
}

export default function NoResultsFoundPlaceholder({ hasAppliedFilters, onClearFilters }: Props) {
    return (
        <Container>
            <Text color="gray" colorLevel={600} size="md">
                No results found
            </Text>
            <Text color="gray" size="sm">
                Try adjusting your search to display data
                {hasAppliedFilters && (
                    <>
                        , or&nbsp;
                        <InlineButton variant="text" onClick={onClearFilters}>
                            clear filters
                        </InlineButton>
                        .
                    </>
                )}
            </Text>
        </Container>
    );
}
