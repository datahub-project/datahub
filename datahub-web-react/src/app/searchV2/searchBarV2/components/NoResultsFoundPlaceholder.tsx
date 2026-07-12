import React, { useMemo } from 'react';
import { Trans, useTranslation } from 'react-i18next';
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

export default function NoResultsFoundPlaceholder({
    hasAppliedFilters,
    hasSelectedView,
    onClearFilters,
    message,
}: Props) {
    const { t } = useTranslation('search');
    const { t: tc } = useTranslation('common.actions');

    const defaultMessage = t('searchBar.noResults.defaultMessage');

    const clearText = useMemo(() => {
        if (hasAppliedFilters && hasSelectedView) {
            return t('searchBar.noResults.clearFiltersAndView');
        }

        if (hasAppliedFilters && !hasSelectedView) {
            return t('searchBar.noResults.clearFilters');
        }

        if (hasSelectedView && !hasAppliedFilters) {
            return t('searchBar.noResults.clearView');
        }

        return undefined;
    }, [hasAppliedFilters, hasSelectedView, t]);

    const resolvedMessage = message ?? defaultMessage;

    return (
        <Container data-testid="no-results-found">
            <Text size="md">{tc('noResults')}</Text>
            <Text size="sm">
                {resolvedMessage}
                {clearText && (
                    <Trans
                        t={t}
                        i18nKey="searchBar.noResults.orClearSuggestion"
                        values={{ clearText }}
                        components={{
                            // eslint-disable-next-line jsx-a11y/control-has-associated-label
                            clickable: (
                                <InlineButton
                                    variant="text"
                                    onClick={onClearFilters}
                                    data-testid="no-results-found-button-clear"
                                />
                            ),
                        }}
                    />
                )}
            </Text>
        </Container>
    );
}
