import React, { MouseEvent } from 'react';
import { Select } from 'antd';
import { CloseCircleFilled } from '@ant-design/icons';
import styled from 'styled-components';
import { Domain, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import ClickOutside from '../../../shared/ClickOutside';
import { BrowserWrapper } from '../../../shared/tags/AddTagsTermsModal';
import useParentSelector from './useParentSelector';
import DomainNavigator from '../../../domain/nestedDomains/domainNavigator/DomainNavigator';
import { useDomainsContext } from '../../../domain/DomainsContext';
import ParentEntities from '../../../search/filters/ParentEntities';
import { getParentDomains } from '../../../domain/utils';

const SearchResultContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
`;

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
    } = useParentSelector({
        entityType: EntityType.Domain,
        entityData,
        selectedParentUrn,
        setSelectedParentUrn,
    });
    const domainSearchResultsFiltered =
        isMoving && domainUrn
            ? searchResults.filter((r) => filterResultsForMove(r.entity as Domain, domainUrn))
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
                showSearch
                allowClear
                clearIcon={<CloseCircleFilled onClick={handleClear} />}
                placeholder="Select"
                filterOption={false}
                value={selectedParentName}
                onSelect={onSelectParent}
                onSearch={handleSearch}
                onFocus={handleFocus}
                dropdownStyle={isShowingDomainNavigator || !searchQuery ? { display: 'none' } : {}}
            >
                {domainSearchResultsFiltered.map((result) => (
                    <Select.Option key={result?.entity?.urn} value={result.entity.urn}>
                        <SearchResultContainer>
                            <ParentEntities parentEntities={getParentDomains(result.entity, entityRegistry)} />
                            {entityRegistry.getDisplayName(result.entity.type, result.entity)}
                        </SearchResultContainer>
                    </Select.Option>
                ))}
            </Select>
            <BrowserWrapper isHidden={!isShowingDomainNavigator}>
                <DomainNavigator
                    domainUrnToHide={isMoving ? domainUrn : undefined}
                    selectDomainOverride={selectDomain}
                />
            </BrowserWrapper>
        </ClickOutside>
    );
}
