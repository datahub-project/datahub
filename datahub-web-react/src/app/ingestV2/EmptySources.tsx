import { EmptyState } from '@components';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Plugs } from '@phosphor-icons/react/dist/csr/Plugs';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { EmptyContainer } from '@app/govern/structuredProperties/styledComponents';

interface Props {
    sourceType?: string;
    isEmptySearchResult?: boolean;
}

const EmptySources = ({ sourceType, isEmptySearchResult }: Props) => {
    const { t } = useTranslation('ingestion');
    return (
        <EmptyContainer>
            {isEmptySearchResult ? (
                <EmptyState
                    icon={MagnifyingGlass}
                    title={t('source.emptySearchTitle')}
                    description={t('source.emptySearchSubtitle')}
                />
            ) : (
                <EmptyState
                    icon={Plugs}
                    title={t('source.emptyTitle', { sourceType: sourceType || t('source.sourcesNoun') })}
                />
            )}
        </EmptyContainer>
    );
};

export default EmptySources;
