import { Icon, Text } from '@components';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import { buildEntityMap } from '@app/entityV2/view/builder/utils';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import { useGetEntities } from '@app/sharedV2/useGetEntities';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { DataHubPageModuleType, Entity } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const ResultsContainer = styled.div`
    overflow-y: auto;
    scrollbar-gutter: stable;
    max-height: 300px;
`;

const EmptyContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 80px;
`;

const GroupLabel = styled(Text)`
    margin-top: 4px;
`;

const StyledIcon = styled(Icon)`
    :hover {
        cursor: pointer;
    }
`;

type Props = {
    selectedUrns: string[];
    onRemoveUrn: (urn: string) => void;
};

/**
 * Displays selected assets grouped by entity type, with remove buttons.
 */
export function SelectedFilterValues({ selectedUrns, onRemoveUrn }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const { entities } = useGetEntities(selectedUrns);

    const entitiesMap = useMemo(() => buildEntityMap(entities), [entities]);

    // Group resolved entities by their entity type for display
    const groupedByType = useMemo(() => {
        const groups: Record<string, Entity[]> = {};
        selectedUrns.forEach((urn) => {
            const entity = entitiesMap[urn];
            if (!entity) return;
            const typeLabel = entityRegistry.getEntityName(entity.type) ?? entity.type;
            if (!groups[typeLabel]) {
                groups[typeLabel] = [];
            }
            groups[typeLabel].push(entity);
        });
        return groups;
    }, [selectedUrns, entitiesMap, entityRegistry]);

    const renderRemoveButton = useCallback(
        (entity: Entity) => {
            return (
                <StyledIcon
                    icon="X"
                    source="phosphor"
                    color="gray"
                    size="md"
                    onClick={(e) => {
                        e.preventDefault();
                        onRemoveUrn(entity.urn);
                    }}
                />
            );
        },
        [onRemoveUrn],
    );

    if (selectedUrns.length === 0) {
        return (
            <Container>
                <Text color="gray" weight="bold">
                    Selected Assets
                </Text>
                <EmptyContainer>
                    <Text color="gray">No assets selected.</Text>
                </EmptyContainer>
            </Container>
        );
    }

    return (
        <Container>
            <Text color="gray" weight="bold">
                Selected Assets
            </Text>
            <ResultsContainer data-testid="selected-filter-values-list">
                {Object.entries(groupedByType).map(([typeLabel, typeEntities]) => (
                    <div key={typeLabel}>
                        <GroupLabel color="gray" size="sm" weight="bold">
                            {typeLabel}
                        </GroupLabel>
                        {typeEntities.map((entity) => (
                            <EntityItem
                                key={entity.urn}
                                entity={entity}
                                customDetailsRenderer={renderRemoveButton}
                                navigateOnlyOnNameClick
                                hideSubtitle
                                moduleType={DataHubPageModuleType.AssetCollection}
                            />
                        ))}
                    </div>
                ))}
            </ResultsContainer>
        </Container>
    );
}
