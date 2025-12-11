/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Dropdown } from 'antd';
import React from 'react';
import styled from 'styled-components';

import {
    SEARCH_FILTER_CONDITION_TYPE_TO_INFO,
    getOperatorOptionsForPredicate,
} from '@app/searchV2/filters/operator/operator';
import { FilterOperatorType, FilterPredicate } from '@app/searchV2/filters/types';

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
        border: 1.5px solid ${(p) => p.theme.styles['primary-color']};
        background-color: ${(p) => p.theme.styles['primary-color']};
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
