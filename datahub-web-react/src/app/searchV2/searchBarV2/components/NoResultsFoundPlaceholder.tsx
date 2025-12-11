/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
    message?: string;
}

const DEFAULT_MESSAGE = "Try adjusting your search to find what you're looking for";
export default function NoResultsFoundPlaceholder({
    hasAppliedFilters,
    hasSelectedView,
    onClearFilters,
    message = DEFAULT_MESSAGE,
}: Props) {
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
                {message}
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
