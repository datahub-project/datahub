import { InfiniteScrollSimpleSelect } from '@components';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useInfiniteScrollStructuredProperties } from '@app/permissions/policy/structuredProperties/useInfiniteScrollStructuredProperties';
import { PropertyOption, StructuredPropertyDefinition } from '@app/permissions/policy/structuredProperties/utils';

import { useGetStructuredPropertyQuery } from '@graphql/structuredProperties.generated';

interface Props {
    selectedPropertyUrn?: string;
    onPropertyChange: (propertyUrn: string) => void;
}

export default function PropertySelectField({ selectedPropertyUrn, onPropertyChange }: Props) {
    const { t } = useTranslation('settings.permissions');
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedQuery, setDebouncedQuery] = useState('');
    const timerRef = useRef<NodeJS.Timeout>();

    useEffect(() => {
        timerRef.current = setTimeout(() => setDebouncedQuery(searchQuery), 300);
        return () => {
            if (timerRef.current) clearTimeout(timerRef.current);
        };
    }, [searchQuery]);

    // Use infinite scroll for properties
    const {
        properties: infiniteProperties,
        scrollRef,
        loading,
        hasMore,
    } = useInfiniteScrollStructuredProperties(debouncedQuery);

    // Fetch selected property if not in infinite scroll results
    const firstPropertyToFetch = useMemo(() => {
        if (!selectedPropertyUrn) return null;
        const isInInfiniteResults = infiniteProperties.some((p) => p.value === selectedPropertyUrn);
        return isInInfiniteResults ? null : selectedPropertyUrn;
    }, [selectedPropertyUrn, infiniteProperties]);

    const { data: selectedPropertyData } = useGetStructuredPropertyQuery({
        variables: { urn: firstPropertyToFetch || '' },
        skip: !firstPropertyToFetch,
        fetchPolicy: 'cache-first',
    });

    const getPropertyOptions = useCallback(() => {
        const optionsArray: PropertyOption[] = [];
        const addedUrns = new Set<string>();

        // Always add selected property at the top
        if (selectedPropertyUrn) {
            const selectedInfiniteProperty = infiniteProperties.find((p) => p.value === selectedPropertyUrn);
            const fetchedSelectedProperty = selectedPropertyData?.entity as StructuredPropertyDefinition;
            const displayName =
                selectedInfiniteProperty?.label ||
                fetchedSelectedProperty?.definition?.displayName ||
                selectedPropertyUrn;

            optionsArray.push({
                value: selectedPropertyUrn,
                label: displayName,
            });
            addedUrns.add(selectedPropertyUrn);
        }

        // Add infinite scroll results, excluding selected property to avoid duplicates
        infiniteProperties.forEach((prop) => {
            if (!addedUrns.has(prop.value)) {
                optionsArray.push(prop);
                addedUrns.add(prop.value);
            }
        });

        return optionsArray;
    }, [infiniteProperties, selectedPropertyUrn, selectedPropertyData]);

    return (
        <InfiniteScrollSimpleSelect
            options={getPropertyOptions()}
            values={selectedPropertyUrn ? [selectedPropertyUrn] : []}
            onUpdate={(selected: string[]) => onPropertyChange(selected?.[0] || '')}
            placeholder={t('privilegeForm.selectProperty')}
            isMultiSelect={false}
            showClear={false}
            width="full"
            showSearch
            onSearchChange={setSearchQuery}
            loading={loading}
            hasMore={hasMore}
            scrollRef={scrollRef}
        />
    );
}
