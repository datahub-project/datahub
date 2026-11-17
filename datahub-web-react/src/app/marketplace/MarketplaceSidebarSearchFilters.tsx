import { Avatar } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';
import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import { AuthorFacetOption, FacetSelectOption } from '@app/document/utils/documentSidebarFacets.utils';
import { isDomain } from '@app/entityV2/domain/utils';
import { isTag } from '@app/entityV2/tag/utils';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';
import TagLink from '@app/sharedV2/tags/TagLink';

import { EntityType } from '@types';

const OwnerOptionRow = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
`;

type Props = {
    selectedDomainUrns: string[];
    selectedOwnerUrns: string[];
    selectedTagUrns: string[];
    domainOptions: FacetSelectOption[];
    ownerOptions: AuthorFacetOption[];
    tagOptions: FacetSelectOption[];
    onDomainsChange: (urns: string[]) => void;
    onOwnersChange: (urns: string[]) => void;
    onTagsChange: (urns: string[]) => void;
};

/**
 * Primary Domain / Owner / Tag multi-selects for the marketplace sidebar.
 * Term and Application live behind "+ Filter".
 */
export default function MarketplaceSidebarSearchFilters({
    selectedDomainUrns,
    selectedOwnerUrns,
    selectedTagUrns,
    domainOptions,
    ownerOptions,
    tagOptions,
    onDomainsChange,
    onOwnersChange,
    onTagsChange,
}: Props) {
    const { t } = useTranslation('misc');

    const showDomainFilter = domainOptions.length > 0 || selectedDomainUrns.length > 0;
    const showOwnerFilter = ownerOptions.length > 0 || selectedOwnerUrns.length > 0;
    const showTagFilter = tagOptions.length > 0 || selectedTagUrns.length > 0;

    return (
        <>
            {showDomainFilter && (
                <SimpleSelect<FacetSelectOption>
                    size="sm"
                    width="fit-content"
                    isMultiSelect
                    showSearch
                    filterResultsByQuery
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
                    dataTestId="marketplace-sidebar-domain-filter"
                />
            )}
            {showOwnerFilter && (
                <SimpleSelect
                    size="sm"
                    width="fit-content"
                    isMultiSelect
                    showSearch
                    filterResultsByQuery
                    placeholder={t('metrics.ownersFilter.placeholder')}
                    selectLabelProps={{ variant: 'labeled', label: t('metrics.ownersFilter.label') }}
                    options={ownerOptions}
                    values={selectedOwnerUrns}
                    onUpdate={onOwnersChange}
                    renderCustomOptionText={(option) => {
                        const { creator } = option as AuthorFacetOption;
                        return (
                            <OwnerOptionRow>
                                <Avatar
                                    name={creator.displayName}
                                    imageUrl={creator.pictureLink ?? undefined}
                                    type={creator.type === EntityType.CorpGroup ? AvatarType.group : AvatarType.user}
                                    showInPill
                                    size="sm"
                                />
                            </OwnerOptionRow>
                        );
                    }}
                    dataTestId="marketplace-sidebar-owners-filter"
                />
            )}
            {showTagFilter && (
                <SimpleSelect<FacetSelectOption>
                    size="sm"
                    width="fit-content"
                    isMultiSelect
                    showSearch
                    filterResultsByQuery
                    placeholder={t('context.tagFilter.placeholder')}
                    selectLabelProps={{ variant: 'labeled', label: t('context.tagFilter.label') }}
                    options={tagOptions}
                    values={selectedTagUrns}
                    onUpdate={onTagsChange}
                    renderCustomOptionText={(option) => {
                        if (!isTag(option.entity)) return option.label;
                        return <TagLink tag={option.entity} enableTooltip={false} enableDrawer={false} />;
                    }}
                    dataTestId="marketplace-sidebar-tag-filter"
                />
            )}
        </>
    );
}
