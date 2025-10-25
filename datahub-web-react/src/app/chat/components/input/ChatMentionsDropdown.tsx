import { Loader, Text, colors } from '@components';
import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import AutoCompleteItem from '@app/searchV2/autoComplete/AutoCompleteItem';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity } from '@types';

interface Props {
    query: string;
    entities: Entity[];
    loading: boolean;
    onSelect: (entity: Entity, displayName: string) => void;
    coordinates: { top: number; left: number };
}

const DropdownContainer = styled.div<{ $top: number; $left: number }>`
    position: fixed;
    bottom: ${(props) => `calc(100vh - ${props.$top}px)`};
    left: ${(props) => props.$left}px;
    width: 400px;
    max-height: 300px;
    overflow-y: auto;
    background-color: white;
    border: 1px solid ${colors.gray[200]};
    border-radius: 8px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    z-index: 10000;
`;

const OptionItem = styled.div<{ $active: boolean }>`
    padding: 8px 12px;
    cursor: pointer;
    background-color: ${(props) => (props.$active ? colors.gray[100] : 'white')};
    transition: background-color 0.1s;

    &:hover {
        background-color: ${colors.gray[100]};
    }

    &:last-child {
        border-bottom-left-radius: 8px;
        border-bottom-right-radius: 8px;
    }
`;

const LoadingContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 24px;
`;

const EmptyContainer = styled.div`
    padding: 24px;
    text-align: center;
`;

export const ChatMentionsDropdown: React.FC<Props> = ({ query, entities, loading, onSelect, coordinates }) => {
    const entityRegistry = useEntityRegistry();
    const [selectedIndex, setSelectedIndex] = useState(0);

    // Reset selected index when entities change
    useEffect(() => {
        setSelectedIndex(0);
    }, [entities]);

    // Handle keyboard navigation
    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            if (entities.length === 0) return;

            if (e.key === 'ArrowDown') {
                e.preventDefault();
                setSelectedIndex((prev) => (prev + 1) % entities.length);
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                setSelectedIndex((prev) => (prev - 1 + entities.length) % entities.length);
            } else if (e.key === 'Enter') {
                e.preventDefault();
                const selectedEntity = entities[selectedIndex];
                if (selectedEntity) {
                    const displayName = entityRegistry.getDisplayName(selectedEntity.type, selectedEntity);
                    onSelect(selectedEntity, displayName);
                }
            }
        };

        document.addEventListener('keydown', handleKeyDown);
        return () => document.removeEventListener('keydown', handleKeyDown);
    }, [entities, selectedIndex, entityRegistry, onSelect]);

    const handleOptionClick = useCallback(
        (entity: Entity) => {
            const displayName = entityRegistry.getDisplayName(entity.type, entity);
            onSelect(entity, displayName);
        },
        [entityRegistry, onSelect],
    );

    if (loading) {
        return (
            <DropdownContainer $top={coordinates.top} $left={coordinates.left}>
                <LoadingContainer>
                    <Loader size="sm" />
                </LoadingContainer>
            </DropdownContainer>
        );
    }

    if (entities.length === 0) {
        return (
            <DropdownContainer $top={coordinates.top} $left={coordinates.left}>
                <EmptyContainer>
                    <Text color="gray">No results found</Text>
                </EmptyContainer>
            </DropdownContainer>
        );
    }

    return (
        <DropdownContainer $top={coordinates.top} $left={coordinates.left}>
            {entities.map((entity, index) => {
                const isActive = selectedIndex === index;

                return (
                    <OptionItem
                        key={entity.urn}
                        $active={isActive}
                        onMouseDown={(e) => {
                            e.preventDefault();
                            handleOptionClick(entity);
                        }}
                        onMouseEnter={() => setSelectedIndex(index)}
                    >
                        <AutoCompleteItem query={query} entity={entity} />
                    </OptionItem>
                );
            })}
        </DropdownContainer>
    );
};
