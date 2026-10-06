import { EmptyState as AlchemyEmptyState } from '@components';
import { ClockCounterClockwise } from '@phosphor-icons/react/dist/csr/ClockCounterClockwise';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { EmptyContainer } from '@app/govern/structuredProperties/styledComponents';

export enum EmptyReasons {
    FILTERS_APPLIED = 'filtersApplied',
    NO_ITEMS = 'noItems',
}

interface Props {
    reason: EmptyReasons;
}

export default function EmptyState({ reason }: Props) {
    const { t } = useTranslation('ingestion');
    const renderContent = () => {
        switch (reason) {
            case EmptyReasons.FILTERS_APPLIED:
                return (
                    <AlchemyEmptyState
                        icon={MagnifyingGlass}
                        title={t('executions.emptyFilteredTitle')}
                        description={t('executions.emptyFilteredSubtitle')}
                    />
                );
            case EmptyReasons.NO_ITEMS:
                return <AlchemyEmptyState icon={ClockCounterClockwise} title={t('executions.emptyTitle')} />;
            default:
                return null;
        }
    };

    return <EmptyContainer>{renderContent()}</EmptyContainer>;
}
