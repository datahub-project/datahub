import React, { useMemo } from 'react';
import styled from 'styled-components';

import { Button, Text } from '@src/alchemy-components';

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
    hasSelectedView?: boolean;
    onClearFilters?: () => void;
}

export default function NoResultsFoundPlaceholder({ hasAppliedFilters, hasSelectedView, onClearFilters }: Props) {
    const clearText = useMemo(() => {
        if (hasAppliedFilters && hasSelectedView) {
            return 'clear filters and selected view';
        }

        if (hasAppliedFilters && !hasSelectedView) {
            return 'clear filters';
        }

        if (hasSelectedView && !hasAppliedFilters) {
            return 'clear selected view';
        }

        return undefined;
    }, [hasAppliedFilters, hasSelectedView]);

    return (
        <Container data-testid="no-results-found">
            <Text color="gray" colorLevel={600} size="md">
                No results found
            </Text>
            <Text color="gray" size="sm">
                Try adjusting your search to find what you&apos;re looking for
                {clearText && (
                    <>
                        , or&nbsp;
                        <InlineButton
                            variant="text"
                            onClick={onClearFilters}
                            data-testid="no-results-found-button-clear"
                        >
                            {clearText}
                        </InlineButton>
                        .
                    </>
                )}
            </Text>
        </Container>
    );
}
