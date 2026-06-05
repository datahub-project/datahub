import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import AdvancedFilterCloseButton from '@app/search/advanced/AdvancedFilterCloseButton';
import { FilterContainer } from '@app/search/advanced/styles';
import { FILTER_DELIMITER } from '@app/search/utils/constants';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, FacetFilterInput } from '@types';

const LIST_SEPARATOR = ', ';

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
    color: ${(props) => props.theme.colors.text};
    margin: 4px;
`;

const ConditionWrapper = styled.div`
    padding: 0 7px;
    border-radius: 5px;
    height: 24px;
    display: flex;
    align-items: center;
    color: ${(props) => props.theme.colors.text};
    background: ${(props) => props.theme.colors.bgSurface};
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
    const { t } = useTranslation('search');
    const { t: tc } = useTranslation('common.labels');
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
        <FilterContainer isCompact={isCompact} isDisabled>
            <FilterWrapper>
                {entityTypes && entityTypes.length > 0 && (
                    <>
                        <FilterFieldLabel>{tc('type')}</FilterFieldLabel>
                        <ConditionWrapper>{t('condition.isAnyOf')}</ConditionWrapper>
                        <FilterValuesWrapper>
                            {entityTypes?.map((entityType, index) => (
                                <>
                                    {entityRegistry.getCollectionName(entityType as EntityType)}
                                    {index !== entityTypes.length - 1 && LIST_SEPARATOR}
                                </>
                            ))}
                        </FilterValuesWrapper>
                    </>
                )}
                {Object.entries(entityTypeToSubTypes).map(([entityType, subTypes]) => (
                    <>
                        <FilterFieldLabel>{tc('type')}</FilterFieldLabel>
                        <ConditionWrapper>{t('condition.is')}</ConditionWrapper>
                        <FilterValuesWrapper>
                            {entityRegistry.getCollectionName(entityType as EntityType)}
                        </FilterValuesWrapper>
                        <FilterFieldLabel>{t('advancedFilter.subType')}</FilterFieldLabel>
                        <ConditionWrapper>{t('condition.isAnyOf')}</ConditionWrapper>
                        <FilterValuesWrapper>
                            {subTypes.map((v, index) => (
                                <>
                                    {capitalizeFirstLetterOnly(v)}
                                    {index !== subTypes.length - 1 && LIST_SEPARATOR}
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
