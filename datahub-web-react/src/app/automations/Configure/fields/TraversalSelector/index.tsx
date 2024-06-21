import React from 'react';

import { Checkbox } from 'antd';

import { CheckboxGroup, CustomCheckboxLabel } from '../../../sharedComponents';

const CheckboxLabel = ({ label, description }: any) => (
    <CustomCheckboxLabel>
        <strong>{label}</strong>
        <p>{description}</p>
    </CustomCheckboxLabel>
);

export const TraversalSelector = () => {
    const options = [
        {
            category: 'Lineage',
            options: [
                {
                    key: 'upstream',
                    name: 'Upstream',
                    description: 'Propagate to upstream lineage',
                },
                {
                    key: 'downstream',
                    name: 'Downstream',
                    description: 'Propagate to downstream lineage',
                },
            ],
        },
        {
            category: 'Hierarchy',
            options: [
                {
                    key: 'parent',
                    name: 'Include Parents',
                    description: 'Propagate to parent hierarchy',
                },
                {
                    key: 'child',
                    name: 'Include Children',
                    description: 'Propagate to child hierarchy',
                },
            ],
        },
    ];

    const handleSelect = (selected) => {
        console.log(selected);
    };

    return (
        <>
            {options.map((option) => (
                <CheckboxGroup key={option.category}>
                    <h4>
                        <strong>{option.category}</strong>
                    </h4>
                    <Checkbox.Group
                        options={option.options.map((opt) => ({
                            label: <CheckboxLabel label={opt.name} description={opt.description} />,
                            value: opt.key,
                        }))}
                        defaultValue={[]}
                        onChange={handleSelect}
                    />
                </CheckboxGroup>
            ))}
        </>
    );
};
