import { Icon } from '@src/alchemy-components';
import { useHydratedEntityMap } from '@src/app/entityV2/shared/tabs/Properties/useHydratedEntityMap';
import { EntityLink } from '@src/app/homeV2/reference/sections/EntityLink';
import { Skeleton } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import { EntityType } from '../../../../../../../types.generated';
import { SearchSelect } from '../../../../../../entityV2/shared/components/styled/search/SearchSelect';
import { EntityAndType } from '../../../../types';
import { extractTypeFromUrn } from '../../../../utils';

const Container = styled.div`
    display: flex;
    width: 100%;
    min-height: 500px;
    height: 70vh;
    border: 1px solid #f0f0f0;
    border-radius: 4px;
`;

const SearchSection = styled.div`
    flex: 2;
    height: 100%;
    overflow: hidden;
    display: flex;
    flex-direction: column;
`;

const SubSearchSection = styled.div`
    flex: 1;
    height: 100%;
    overflow: hidden;
    display: flex;
    flex-direction: column;
`;

const CurrentSection = styled.div`
    flex: 1;
    width: 40%;
    border-left: 1px solid #f0f0f0;
    display: flex;
    flex-direction: column;
`;

const SectionHeader = styled.div`
    padding-left: 20px;
    margin-top: 10px;
    font-size: 16px;
    font-weight: 500;
    color: #666;
`;

const ScrollableContent = styled.div`
    flex: 1;
    overflow: auto;
    padding: 20px;
`;

const SelectedItem = styled.div`
    display: flex;
    padding: 8px;
    border-radius: 4px;
    margin-bottom: 8px;
    align-items: center;
    border: 1px solid #f0f0f0;
    justify-content: space-between;

    &:hover {
        background-color: #fafafa;
    }
`;

const IconWrapper = styled.div`
    cursor: pointer;
`;

const INITIAL_SELECTED_ENTITIES = [] as EntityAndType[];

// New interface for the generic component
interface SearchSelectUrnInputProps {
    allowedEntityTypes: EntityType[];
    isMultiple: boolean;
    selectedValues: string[];
    updateSelectedValues: (values: string[] | number[]) => void;
}

// Generic component that doesn't depend on StructuredProperty
export function SearchSelectUrnInput({
    allowedEntityTypes,
    isMultiple,
    selectedValues,
    updateSelectedValues,
}: SearchSelectUrnInputProps) {
    const [tempSelectedEntities, setTempSelectedEntities] = useState<EntityAndType[]>(INITIAL_SELECTED_ENTITIES);

    // Convert the selected values (urns) to EntityAndType format for SearchSelect
    const selectedEntities = useMemo(
        () => selectedValues.map((urn) => ({ urn, type: extractTypeFromUrn(urn) })),
        [selectedValues],
    );

    // only run this once
    useEffect(() => {
        if (tempSelectedEntities === INITIAL_SELECTED_ENTITIES) {
            setTempSelectedEntities(selectedEntities);
        }
    }, [selectedEntities, setTempSelectedEntities, tempSelectedEntities]);

    // call updateSelectedValues when tempSelectedEntities changes
    useEffect(() => {
        if (tempSelectedEntities !== INITIAL_SELECTED_ENTITIES) {
            updateSelectedValues(tempSelectedEntities.map((entity) => entity.urn));
        }
        // this is excluded from the dependency array because updateSelectedValues is reconstructed
        // by the parent component on every render
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [tempSelectedEntities]);

    const removeEntity = (entity: EntityAndType) => {
        setTempSelectedEntities(tempSelectedEntities.filter((e) => e.urn !== entity.urn));
    };

    const hydratedEntityMap = useHydratedEntityMap(tempSelectedEntities.map((entity) => entity.urn));

    return (
        <Container>
            <SearchSection>
                <SectionHeader>Search Values</SectionHeader>
                <SubSearchSection>
                    <SearchSelect
                        fixedEntityTypes={allowedEntityTypes}
                        selectedEntities={tempSelectedEntities}
                        setSelectedEntities={setTempSelectedEntities}
                        limit={isMultiple ? undefined : 1}
                    />
                </SubSearchSection>
            </SearchSection>
            <CurrentSection>
                <SectionHeader>Selected Values</SectionHeader>
                <ScrollableContent>
                    {tempSelectedEntities.length === 0 ? (
                        <div>No values selected</div>
                    ) : (
                        tempSelectedEntities.map((entity) => (
                            <SelectedItem key={entity.urn}>
                                {hydratedEntityMap[entity.urn] ? (
                                    <EntityLink entity={hydratedEntityMap[entity.urn]} />
                                ) : (
                                    <Skeleton.Input active />
                                )}
                                <IconWrapper>
                                    <Icon icon="X" source="phosphor" onClick={() => removeEntity(entity)} />
                                </IconWrapper>
                            </SelectedItem>
                        ))
                    )}
                </ScrollableContent>
            </CurrentSection>
        </Container>
    );
}
