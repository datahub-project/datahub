import React from 'react';
import { useTranslation } from 'react-i18next';

import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import { FacetSelectOption } from '@app/document/utils/documentSidebarFacets.utils';
import { isDomain } from '@app/entityV2/domain/utils';
import { isGlossaryTerm } from '@app/entityV2/glossaryTerm/utils';
import { isTag } from '@app/entityV2/tag/utils';
import GlossaryTermPill from '@app/glossaryV2/GlossaryTermPill';
import { getGlossaryTermColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';
import TagLink from '@app/sharedV2/tags/TagLink';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, GlossaryTerm } from '@types';

type Props = {
    selectedTypeNames: string[];
    selectedDomainUrns: string[];
    selectedTagUrns: string[];
    selectedTermUrns: string[];
    typeOptions: FacetSelectOption[];
    domainOptions: FacetSelectOption[];
    tagOptions: FacetSelectOption[];
    termOptions: FacetSelectOption[];
    onTypesChange: (values: string[]) => void;
    onDomainsChange: (urns: string[]) => void;
    onTagsChange: (urns: string[]) => void;
    onTermsChange: (urns: string[]) => void;
    /** Tag starts behind "+ Filter"; Domain / Term / Type are always shown. */
    showTagFilter?: boolean;
    /** Open a just-promoted Tag dropdown so the user can pick immediately. */
    defaultOpenTagFilter?: boolean;
    /** Remount key so defaultOpen applies when Tag is added from "+ Filter". */
    tagFilterKey?: string;
};

/**
 * Primary Domain / Term / Type multi-selects (+ optional Tag) for the documents sidebar.
 * Options come from useDocumentSidebarFacetOptions (lifted to ContextSidebar so
 * Author / Source share the same aggregation pass).
 */
export default function DocumentSidebarSearchFilters({
    selectedTypeNames,
    selectedDomainUrns,
    selectedTagUrns,
    selectedTermUrns,
    typeOptions,
    domainOptions,
    tagOptions,
    termOptions,
    onTypesChange,
    onDomainsChange,
    onTagsChange,
    onTermsChange,
    showTagFilter = false,
    defaultOpenTagFilter = false,
    tagFilterKey = 'tag',
}: Props) {
    const { t } = useTranslation('misc');
    const entityRegistry = useEntityRegistry();
    const generateGlossaryColor = useGenerateGlossaryColorFromPalette();

    return (
        <>
            <SimpleSelect<FacetSelectOption>
                size="sm"
                width="fit-content"
                isMultiSelect
                showSearch
                filterResultsByQuery
                isDisabled={domainOptions.length === 0 && selectedDomainUrns.length === 0}
                placeholder={t('context.domainFilter.placeholder')}
                selectLabelProps={{ variant: 'labeled', label: t('context.domainFilter.label') }}
                options={domainOptions}
                values={selectedDomainUrns}
                onUpdate={onDomainsChange}
                renderCustomOptionText={(option) => {
                    if (!isDomain(option.entity)) return option.label;
                    return (
                        <DomainLink
                            domain={option.entity}
                            readOnly
                            enableTooltip={false}
                            iconSize={20}
                            iconFontSize={12}
                        />
                    );
                }}
                dataTestId="context-sidebar-domain-filter"
            />
            <SimpleSelect<FacetSelectOption>
                size="sm"
                width="fit-content"
                isMultiSelect
                showSearch
                filterResultsByQuery
                isDisabled={termOptions.length === 0 && selectedTermUrns.length === 0}
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
                dataTestId="context-sidebar-term-filter"
            />
            <SimpleSelect
                size="sm"
                width="fit-content"
                isMultiSelect
                showSearch
                filterResultsByQuery
                isDisabled={typeOptions.length === 0 && selectedTypeNames.length === 0}
                placeholder={t('context.typeFilter.placeholder')}
                selectLabelProps={{ variant: 'labeled', label: t('context.typeFilter.label') }}
                options={typeOptions}
                values={selectedTypeNames}
                onUpdate={onTypesChange}
                dataTestId="context-sidebar-type-filter"
            />
            {showTagFilter && (
                <SimpleSelect<FacetSelectOption>
                    key={tagFilterKey}
                    size="sm"
                    width="fit-content"
                    isMultiSelect
                    showSearch
                    filterResultsByQuery
                    defaultOpen={defaultOpenTagFilter}
                    isDisabled={tagOptions.length === 0 && selectedTagUrns.length === 0 && !defaultOpenTagFilter}
                    placeholder={t('context.tagFilter.placeholder')}
                    selectLabelProps={{ variant: 'labeled', label: t('context.tagFilter.label') }}
                    options={tagOptions}
                    values={selectedTagUrns}
                    onUpdate={onTagsChange}
                    renderCustomOptionText={(option) => {
                        if (!isTag(option.entity)) return option.label;
                        return <TagLink tag={option.entity} enableTooltip={false} enableDrawer={false} />;
                    }}
                    dataTestId="context-sidebar-tag-filter"
                />
            )}
        </>
    );
}
