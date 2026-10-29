import { EmptyState } from '@components';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Plugs } from '@phosphor-icons/react/dist/csr/Plugs';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { EmptyContainer } from '@app/govern/structuredProperties/styledComponents';

interface Props {
    sourceType?: string;
    isEmptySearchResult?: boolean;
    isEmptyLastRunResult?: boolean;
}

const EmptySources = ({ sourceType, isEmptySearchResult, isEmptyLastRunResult }: Props) => {
    const { t } = useTranslation('ingestion');

    if (isEmptySearchResult) {
        return (
            <EmptyContainer>
                <EmptyState
                    icon={MagnifyingGlass}
                    title={t('source.emptySearchTitle')}
                    description={t('source.emptySearchSubtitle')}
                />
            </EmptyContainer>
        );
    }
    if (isEmptyLastRunResult) {
        // Latest-run fields are filled in after an upgrade and as runs are recorded, so a source that
        // has run can be missing from the results for a while.
        return (
            <EmptyContainer>
                <EmptyState
                    icon={MagnifyingGlass}
                    title={t('source.emptyLastRunFilterTitle')}
                    description={t('source.emptyLastRunFilterSubtitle')}
                />
            </EmptyContainer>
        );
    }
    return (
        <EmptyContainer>
            <EmptyState
                icon={Plugs}
                title={t('source.emptyTitle', { sourceType: sourceType || t('source.sourcesNoun') })}
            />
        </EmptyContainer>
    );
};

export default EmptySources;
