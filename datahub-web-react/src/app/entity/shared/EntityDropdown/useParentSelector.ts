import { useEffect, useState } from 'react';
import { useEntityData } from '../EntityContext';
import { useGetSearchResultsLazyQuery } from '../../../../graphql/search.generated';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';

interface Props {
    entityType: EntityType;
    selectedParentUrn: string;
    setSelectedParentUrn: (parent: string) => void;
}

export default function useParentSelector({ entityType, selectedParentUrn, setSelectedParentUrn }: Props) {
    const [selectedParentName, setSelectedParentName] = useState('');
    const [isFocusedOnInput, setIsFocusedOnInput] = useState(false);
    const [searchQuery, setSearchQuery] = useState('');
    const entityRegistry = useEntityRegistry();
    const { entityData, urn: entityDataUrn } = useEntityData();

    const [search, { data }] = useGetSearchResultsLazyQuery();
    const searchResults = data?.search?.searchResults || [];

    useEffect(() => {
        if (entityData && selectedParentUrn === entityDataUrn) {
            const displayName = entityRegistry.getDisplayName(entityType, entityData);
            setSelectedParentName(displayName);
        }
    }, [entityData, entityRegistry, selectedParentUrn, entityDataUrn, entityType]);

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
        setSelectedParentName('');
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
