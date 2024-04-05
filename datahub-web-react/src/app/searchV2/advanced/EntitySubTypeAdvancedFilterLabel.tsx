import React, { useMemo } from 'react';
import styled from 'styled-components';
import { FILTER_DELIMITER } from '../utils/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { EntityType, FacetFilterInput } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import AdvancedFilterCloseButton from './AdvancedFilterCloseButton';
import { FilterContainer } from './styles';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';

const FilterFieldLabel = styled.span`
    font-weight: 600;
    margin-right: 4px;
`;

const FilterWrapper = styled.span`
    padding: 2px 4px;
    display: flex;
    align-items: center;
`;

const FilterValuesWrapper = styled.div`
    color: ${ANTD_GRAY[9]};
    margin: 4px;
`;

const ConditionWrapper = styled.div`
    padding: 0 7px;
    border-radius: 5px;
    height: 24px;
    display: flex;
    align-items: center;
    color: ${ANTD_GRAY[9]};
    background: ${ANTD_GRAY[2]};
`;

function getEntityTypeToSubType(nestedSubTypes?: string[]): { [key: string]: string[] } {
    const entityTypeToSubTypes = {};
    nestedSubTypes?.forEach((nestedSubType) => {
        const [entityType, subType] = nestedSubType.split(FILTER_DELIMITER);
        entityTypeToSubTypes[entityType] = [...(entityTypeToSubTypes[entityType] || []), subType];
    });
    return entityTypeToSubTypes;
}

interface Props {
    filter: FacetFilterInput;
    isCompact: boolean;
    disabled: boolean;
    onClose: () => void;
}

export default function EntitySubTypeAdvancedFilterLabel({ filter, isCompact, disabled, onClose }: Props) {
    const entityRegistry = useEntityRegistry();
    const entityTypes = useMemo(
        () => filter.values?.filter((value) => !value.includes(FILTER_DELIMITER)),
        [filter.values],
    );
    const nestedSubTypes = useMemo(
        () => filter.values?.filter((value) => value.includes(FILTER_DELIMITER)),
        [filter.values],
    );
    const entityTypeToSubTypes = useMemo(() => getEntityTypeToSubType(nestedSubTypes), [nestedSubTypes]);

    return (
        <FilterContainer $isCompact={isCompact} isDisabled>
            <FilterWrapper>
                {entityTypes && entityTypes.length > 0 && (
                    <>
                        <FilterFieldLabel>Type</FilterFieldLabel>
                        <ConditionWrapper>is any of</ConditionWrapper>
                        <FilterValuesWrapper>
                            {entityTypes?.map((entityType, index) => (
                                <>
                                    {entityRegistry.getCollectionName(entityType as EntityType)}
                                    {index !== entityTypes.length - 1 && ', '}
                                </>
                            ))}
                        </FilterValuesWrapper>
                    </>
                )}
                {Object.entries(entityTypeToSubTypes).map(([entityType, subTypes]) => (
                    <>
                        <FilterFieldLabel>Type</FilterFieldLabel>
                        <ConditionWrapper>is</ConditionWrapper>
                        <FilterValuesWrapper>
                            {entityRegistry.getCollectionName(entityType as EntityType)}
                        </FilterValuesWrapper>
                        <FilterFieldLabel>SubType</FilterFieldLabel>
                        <ConditionWrapper>is any of</ConditionWrapper>
                        <FilterValuesWrapper>
                            {subTypes.map((v, index) => (
                                <>
                                    {capitalizeFirstLetterOnly(v)}
                                    {index !== subTypes.length - 1 && ', '}
                                </>
                            ))}
                        </FilterValuesWrapper>
                    </>
                ))}
                {!disabled && <AdvancedFilterCloseButton onClose={onClose} />}
            </FilterWrapper>
        </FilterContainer>
    );
}
