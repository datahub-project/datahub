import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import { isDomain } from '@app/entityV2/domain/utils';
import { isGlossaryTerm } from '@app/entityV2/glossaryTerm/utils';
import { isTag } from '@app/entityV2/tag/utils';
import GlossaryTermPill from '@app/glossaryV2/GlossaryTermPill';
import { getGlossaryTermColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { FacetSelectOption, isDataPlatformEntity } from '@app/metrics/hooks/useMetricsSidebarFacetOptions';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';
import TagLink from '@app/sharedV2/tags/TagLink';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, GlossaryTerm } from '@types';

const PlatformOptionRow = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
`;

type Props = {
    selectedPlatformUrns: string[];
    selectedDomainUrns: string[];
    selectedTagUrns: string[];
    selectedTermUrns: string[];
    platformOptions: FacetSelectOption[];
    domainOptions: FacetSelectOption[];
    tagOptions: FacetSelectOption[];
    termOptions: FacetSelectOption[];
    onPlatformsChange: (urns: string[]) => void;
    onDomainsChange: (urns: string[]) => void;
    onTagsChange: (urns: string[]) => void;
    onTermsChange: (urns: string[]) => void;
    /** Tag starts behind "+ Filter"; Platform / Domain / Term are always shown. */
    showTagFilter?: boolean;
    defaultOpenTagFilter?: boolean;
    tagFilterKey?: string;
};

/**
 * Primary Platform / Domain / Term multi-selects (+ optional Tag) for the metrics sidebar.
 */
export default function MetricsSidebarSearchFilters({
    selectedPlatformUrns,
    selectedDomainUrns,
    selectedTagUrns,
    selectedTermUrns,
    platformOptions,
    domainOptions,
    tagOptions,
    termOptions,
    onPlatformsChange,
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

    const platformSelectOptions = platformOptions.map((option) => ({
        ...option,
        icon: isDataPlatformEntity(option.entity) ? (
            <PlatformIcon
                platform={option.entity}
                size={14}
                styles={{ backgroundColor: 'transparent', padding: '0px', borderRadius: '0px' }}
            />
        ) : undefined,
    }));

    return (
        <>
            <SimpleSelect
                size="sm"
                width="fit-content"
                isMultiSelect
                showSearch
                filterResultsByQuery
                isDisabled={platformSelectOptions.length === 0 && selectedPlatformUrns.length === 0}
                placeholder={t('metrics.platformFilter.placeholder')}
                selectLabelProps={{ variant: 'labeled', label: t('metrics.platformFilter.label') }}
                options={platformSelectOptions}
                values={selectedPlatformUrns}
                onUpdate={onPlatformsChange}
                renderCustomOptionText={(option) => (
                    <PlatformOptionRow>
                        {option.icon}
                        <span>{option.label}</span>
                    </PlatformOptionRow>
                )}
                dataTestId="metrics-sidebar-platform-filter"
            />
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
                dataTestId="metrics-sidebar-domain-filter"
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
                dataTestId="metrics-sidebar-term-filter"
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
                    dataTestId="metrics-sidebar-tag-filter"
                />
            )}
        </>
    );
}
