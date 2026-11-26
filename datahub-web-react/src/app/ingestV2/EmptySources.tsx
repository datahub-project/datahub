import { EmptyState } from '@components';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Plugs } from '@phosphor-icons/react/dist/csr/Plugs';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { EmptyContainer } from '@app/govern/structuredProperties/styledComponents';

export type EmptySourcesReason = 'search' | 'lastRunFilter';

interface Props {
    sourceType?: string;
    emptyReason?: EmptySourcesReason;
}

const EmptySources = ({ sourceType, emptyReason }: Props) => {
    const { t } = useTranslation('ingestion');

    if (emptyReason === 'search') {
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
    if (emptyReason === 'lastRunFilter') {
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
