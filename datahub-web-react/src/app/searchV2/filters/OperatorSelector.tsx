import { Dropdown } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { SEARCH_COLORS } from '../../entityV2/shared/constants';
import { SEARCH_FILTER_CONDITION_TYPE_TO_INFO, getOperatorOptionsForPredicate } from './operator/operator';
import { FilterOperatorType, FilterPredicate } from './types';

const OptionContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
`;

const Icon = styled.div`
    margin-right: 8px;
`;

const SelectedOperatorText = styled.div`
    cursor: pointer;
    border-radius: 6px;
    border: 1.5px solid transparent;
    padding: 2px;
    :hover {
        border: 1.5px solid ${SEARCH_COLORS.TITLE_PURPLE};
        background-color: ${SEARCH_COLORS.TITLE_PURPLE};
        color: #fff;
    }
`;

const Text = styled.div``;

interface Props {
    predicate: FilterPredicate;
    onChangeOperator: (operator: FilterOperatorType) => void;
}

export default function OperatorSelector({ predicate, onChangeOperator }: Props) {
    const isPlural = predicate.values?.length > 1;
    const operatorOptions = getOperatorOptionsForPredicate(predicate, isPlural);
    const selectedOperator = SEARCH_FILTER_CONDITION_TYPE_TO_INFO.get(predicate.operator);
    const selectedOperatorText = (isPlural && selectedOperator?.pluralText) || selectedOperator?.text;

    const items = [
        ...operatorOptions
            .filter((op) => op)
            .map((operatorOption) => {
                return {
                    key: operatorOption.type,
                    label: (
                        <OptionContainer
                            key={operatorOption.type}
                            onClick={() => onChangeOperator(operatorOption.type)}
                        >
                            <Icon>{operatorOption.icon}</Icon>
                            <Text>{(isPlural && operatorOption.pluralText) || operatorOption.text}</Text>
                        </OptionContainer>
                    ),
                };
            }),
    ];

    return (
        <Dropdown trigger={['click']} menu={{ items }}>
            <SelectedOperatorText>{selectedOperatorText}</SelectedOperatorText>
        </Dropdown>
    );
}
