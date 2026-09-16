import React from 'react';
import { useTranslation } from 'react-i18next';

import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import { FacetSelectOption } from '@app/document/utils/documentSidebarFacets.utils';
import { isGlossaryTerm } from '@app/entityV2/glossaryTerm/utils';
import GlossaryTermPill from '@app/glossaryV2/GlossaryTermPill';
import { getGlossaryTermColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { SecondaryBrowseFilter } from '@app/marketplace/utils/marketplaceSidebarMode';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, GlossaryTerm } from '@types';

type Props = {
    promotedBrowseFilters: ReadonlySet<SecondaryBrowseFilter>;
    filterToAutoOpen: SecondaryBrowseFilter | null;
    autoOpenNonce: number;
    selectedTermUrns: string[];
    selectedApplicationUrns: string[];
    termOptions: FacetSelectOption[];
    applicationOptions: FacetSelectOption[];
    onTermsChange: (urns: string[]) => void;
    onApplicationsChange: (urns: string[]) => void;
};

/**
 * Promoted Term / Application multi-selects for the marketplace sidebar "+ Filter" row.
 */
export default function MarketplaceSidebarSecondaryFilters({
    promotedBrowseFilters,
    filterToAutoOpen,
    autoOpenNonce,
    selectedTermUrns,
    selectedApplicationUrns,
    termOptions,
    applicationOptions,
    onTermsChange,
    onApplicationsChange,
}: Props) {
    const { t } = useTranslation('misc');
    const entityRegistry = useEntityRegistry();
    const generateGlossaryColor = useGenerateGlossaryColorFromPalette();

    return (
        <>
            {promotedBrowseFilters.has('term') && (
                <SimpleSelect
                    key={filterToAutoOpen === 'term' ? `term-${autoOpenNonce}` : 'term'}
                    size="sm"
                    width="fit-content"
                    isMultiSelect
                    showSearch
                    filterResultsByQuery
                    defaultOpen={filterToAutoOpen === 'term'}
                    isDisabled={
                        termOptions.length === 0 && selectedTermUrns.length === 0 && filterToAutoOpen !== 'term'
                    }
                    placeholder={t('context.termFilter.placeholder')}
                    selectLabelProps={{ variant: 'labeled', label: t('context.termFilter.label') }}
                    options={termOptions}
                    values={selectedTermUrns}
                    onUpdate={onTermsChange}
                    renderCustomOptionText={(option) => {
                        if (!isGlossaryTerm(option.entity)) return option.label;
                        const term = option.entity as GlossaryTerm;
                        const displayName = entityRegistry.getDisplayName(EntityType.GlossaryTerm, term);
                        const termColor = getGlossaryTermColor(term, generateGlossaryColor);
                        return <GlossaryTermPill name={displayName} color={termColor} size="sm" />;
                    }}
                    dataTestId="marketplace-sidebar-term-filter"
                />
            )}
            {promotedBrowseFilters.has('application') && (
                <SimpleSelect
                    key={filterToAutoOpen === 'application' ? `application-${autoOpenNonce}` : 'application'}
                    size="sm"
                    width="fit-content"
                    isMultiSelect
                    showSearch
                    filterResultsByQuery
                    defaultOpen={filterToAutoOpen === 'application'}
                    isDisabled={
                        applicationOptions.length === 0 &&
                        selectedApplicationUrns.length === 0 &&
                        filterToAutoOpen !== 'application'
                    }
                    placeholder={t('marketplace.applicationFilter.placeholder')}
                    selectLabelProps={{ variant: 'labeled', label: t('marketplace.filterApplication') }}
                    options={applicationOptions}
                    values={selectedApplicationUrns}
                    onUpdate={onApplicationsChange}
                    dataTestId="marketplace-sidebar-application-filter"
                />
            )}
        </>
    );
}
