import React from 'react';
import { Select } from 'antd';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import ClickOutside from '../../../shared/ClickOutside';
import { BrowserWrapper } from '../../../shared/tags/AddTagsTermsModal';
import useParentSelector from './useParentSelector';
import DomainNavigator from '../../../domain/nestedDomains/domainNavigator/DomainNavigator';

interface Props {
    selectedParentUrn: string;
    setSelectedParentUrn: (parent: string) => void;
}

export default function DomainParentSelect(props: Props) {
    const { selectedParentUrn, setSelectedParentUrn } = props;
    const entityRegistry = useEntityRegistry();

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
        selectedParentUrn,
        setSelectedParentUrn,
    });

    // TODO - select from modal
    console.log(selectParentFromBrowser);

    const isShowingDomainNavigator = !searchQuery && isFocusedOnInput;

    return (
        <ClickOutside onClickOutside={() => setIsFocusedOnInput(false)}>
            <Select
                showSearch
                allowClear
                filterOption={false}
                value={selectedParentName}
                onSelect={onSelectParent}
                onSearch={handleSearch}
                onClear={clearSelectedParent}
                onFocus={() => setIsFocusedOnInput(true)}
                dropdownStyle={isShowingDomainNavigator || !searchQuery ? { display: 'none' } : {}}
            >
                {searchResults?.map((result) => (
                    <Select.Option key={result?.entity?.urn} value={result.entity.urn}>
                        {entityRegistry.getDisplayName(result.entity.type, result.entity)}
                    </Select.Option>
                ))}
            </Select>
            <BrowserWrapper isHidden={!isShowingDomainNavigator}>
                <DomainNavigator />
            </BrowserWrapper>
        </ClickOutside>
    );
}
