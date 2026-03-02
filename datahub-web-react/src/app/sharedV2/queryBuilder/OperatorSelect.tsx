import { SelectOption, SimpleSelect } from '@components';
import React, { useMemo } from 'react';

import { Operator } from '@app/sharedV2/queryBuilder/builder/property/types/operators';
import { ConditionElementWithFixedWidth } from '@app/sharedV2/queryBuilder/styledComponents';

interface Props {
    selectedOperator?: string;
    operators?: Operator[];
    onChangeOperator: (newOperatorId) => void;
}

const OperatorSelect = ({ selectedOperator, operators, onChangeOperator }: Props) => {
    const options: SelectOption[] = useMemo(
        () =>
            operators?.map((operator) => ({
                value: operator.id.toString(),
                label: operator.displayName,
                description: operator.description,
            })) ?? [],
        [operators],
    );

    return (
        <ConditionElementWithFixedWidth>
            <SimpleSelect
                options={options}
                placeholder="Select an operator..."
                onUpdate={(val) => onChangeOperator(val[0])}
                values={selectedOperator ? [selectedOperator.toLowerCase()] : []}
                isDisabled={!operators}
                dataTestId="condition-operator-select"
                width="full"
                showClear={false}
            />
        </ConditionElementWithFixedWidth>
    );
};

export default OperatorSelect;
