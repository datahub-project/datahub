import { useEffect, useState } from 'react';
import { useGetSearchResultsLazyQuery } from '../../../../graphql/search.generated';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { GenericEntityProperties } from '../types';

interface Props {
    entityType: EntityType;
    entityData: GenericEntityProperties | null;
    selectedParentUrn: string;
    setSelectedParentUrn: (parent: string) => void;
}

export default function useParentSelector({ entityType, entityData, selectedParentUrn, setSelectedParentUrn }: Props) {
    const [selectedParentName, setSelectedParentName] = useState<string>();
    const [isFocusedOnInput, setIsFocusedOnInput] = useState(false);
    const [searchQuery, setSearchQuery] = useState('');
    const entityRegistry = useEntityRegistry();

    const [search, { data }] = useGetSearchResultsLazyQuery();
    const searchResults = data?.search?.searchResults || [];

    useEffect(() => {
        if (entityData && selectedParentUrn === entityData.urn) {
            const displayName = entityRegistry.getDisplayName(entityType, entityData);
            setSelectedParentName(displayName);
        }
    }, [entityData, entityRegistry, selectedParentUrn, entityData?.urn, entityType]);

    function handleSearch(text: string) {
        setSearchQuery(text);
        search({
            variables: {
                input: {
                    type: entityType,
                    query: text,
                    start: 0,
                    count: 5,
                },
            },
        });
    }

    function onSelectParent(parentUrn: string) {
        const selectedParent = searchResults.find((result) => result.entity.urn === parentUrn);
        if (selectedParent) {
            setSelectedParentUrn(parentUrn);
            const displayName = entityRegistry.getDisplayName(selectedParent.entity.type, selectedParent.entity);
            setSelectedParentName(displayName);
        }
    }

    function clearSelectedParent() {
        setSelectedParentUrn('');
        setSelectedParentName(undefined);
        setSearchQuery('');
    }

    function selectParentFromBrowser(urn: string, displayName: string) {
        setIsFocusedOnInput(false);
        setSelectedParentUrn(urn);
        setSelectedParentName(displayName);
    }

    return {
        searchQuery,
        searchResults,
        isFocusedOnInput,
        selectedParentName,
        onSelectParent,
        handleSearch,
        setIsFocusedOnInput,
        selectParentFromBrowser,
        clearSelectedParent,
    };
}
