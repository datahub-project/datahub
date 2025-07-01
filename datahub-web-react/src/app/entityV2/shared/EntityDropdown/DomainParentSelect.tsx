import { CloseCircleFilled } from '@ant-design/icons';
import { Empty, Select } from 'antd';
import React, { MouseEvent } from 'react';

import DomainNavigator from '@app/domain/nestedDomains/domainNavigator/DomainNavigator';
import domainAutocompleteOptions from '@app/domainV2/DomainAutocompleteOptions';
import useParentSelector from '@app/entityV2/shared/EntityDropdown/useParentSelector';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import ClickOutside from '@app/shared/ClickOutside';
import { BrowserWrapper } from '@app/shared/tags/AddTagsTermsModal';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useDomainsContext } from '@src/app/domainV2/DomainsContext';

import { Domain, EntityType } from '@types';

// filter out entity itself and its children
export function filterResultsForMove(entity: Domain, entityUrn: string) {
    return (
        entity.urn !== entityUrn &&
        entity.__typename === 'Domain' &&
        !entity.parentDomains?.domains?.some((node) => node.urn === entityUrn)
    );
}

interface Props {
    selectedParentUrn: string;
    setSelectedParentUrn: (parent: string) => void;
    isMoving?: boolean;
}

export default function DomainParentSelect({ selectedParentUrn, setSelectedParentUrn, isMoving }: Props) {
    const entityRegistry = useEntityRegistry();
    const { entityData } = useDomainsContext();
    const domainUrn = entityData?.urn;

    const {
        searchResults,
        searchQuery,
        isFocusedOnInput,
        selectedParentName,
        selectParentFromBrowser,
        onSelectParent,
        handleSearch,
        clearSelectedParent,
        setIsFocusedOnInput,
        autoCompleteResultsLoading,
    } = useParentSelector({
        entityType: EntityType.Domain,
        entityData,
        selectedParentUrn,
        setSelectedParentUrn,
    });
    const domainSearchResultsFiltered =
        isMoving && domainUrn
            ? searchResults.filter((r) => filterResultsForMove(r as Domain, domainUrn))
            : searchResults;

    function selectDomain(domain: Domain) {
        selectParentFromBrowser(domain.urn, entityRegistry.getDisplayName(EntityType.Domain, domain));
    }

    const isShowingDomainNavigator = !searchQuery && isFocusedOnInput;

    const handleFocus = () => setIsFocusedOnInput(true);
    const handleClickOutside = () => setIsFocusedOnInput(false);

    const handleClear = (event: MouseEvent) => {
        // Prevent, otherwise antd will close the select menu but leaves it focused
        event.stopPropagation();
        clearSelectedParent();
    };

    return (
        <ClickOutside onClickOutside={handleClickOutside}>
            <Select
                autoFocus
                showSearch
                allowClear
                clearIcon={<CloseCircleFilled onClick={handleClear} />}
                filterOption={false}
                defaultActiveFirstOption={false}
                placeholder="Select"
                value={selectedParentName}
                onSelect={onSelectParent}
                onSearch={handleSearch}
                onFocus={handleFocus}
                dropdownStyle={isShowingDomainNavigator || !searchQuery ? { display: 'none' } : {}}
                notFoundContent={
                    <Empty
                        description="No Domains Found"
                        image={Empty.PRESENTED_IMAGE_SIMPLE}
                        style={{ color: ANTD_GRAY[7] }}
                    />
                }
                options={domainAutocompleteOptions(
                    domainSearchResultsFiltered,
                    autoCompleteResultsLoading,
                    entityRegistry,
                )}
            />
            <BrowserWrapper isHidden={!isShowingDomainNavigator}>
                <DomainNavigator
                    domainUrnToHide={isMoving ? domainUrn : undefined}
                    selectDomainOverride={selectDomain}
                />
            </BrowserWrapper>
        </ClickOutside>
    );
}
