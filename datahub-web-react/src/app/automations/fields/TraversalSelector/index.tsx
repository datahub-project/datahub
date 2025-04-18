import { Checkbox } from 'antd';
import React from 'react';

import { TRAVERSAL_OPTIONS, TraversalTypes } from '@app/automations/fields/TraversalSelector/constants';
import { CustomCheckboxLabel } from '@app/automations/sharedComponents';
import type { ComponentBaseProps } from '@app/automations/types';

// State Type (ensures the state is correctly applied across templates)
export type TraversalSelectorStateType = {
    lineage: TraversalTypes[];
    hierarchy: TraversalTypes[];
};

// Custom Checkbox w/ Label component
const CheckboxLabel = ({ label, description }: any) => (
    <CustomCheckboxLabel>
        <strong>{label}</strong>
        <p>{description}</p>
    </CustomCheckboxLabel>
);

// Component
export const TraversalSelector = ({ state, passStateToParent }: ComponentBaseProps) => {
    // Defined in @app/automations/fields/index
    const { lineage, hierarchy } = state as TraversalSelectorStateType;

    // Return the checkboxes for lineage and hierarchy
    return (
        <>
            <h4>
                <strong>Lineage</strong>
            </h4>
            <Checkbox.Group
                options={TRAVERSAL_OPTIONS.lineage.map((opt) => ({
                    label: <CheckboxLabel label={opt.name} description={opt.description} />,
                    value: opt.key,
                }))}
                defaultValue={lineage}
                onChange={(value) => passStateToParent({ lineage: value })}
            />

            <h4>
                <strong>Hierarchy</strong>
            </h4>
            <Checkbox.Group
                options={TRAVERSAL_OPTIONS.hierarchy.map((opt) => ({
                    label: <CheckboxLabel label={opt.name} description={opt.description} />,
                    value: opt.key,
                }))}
                defaultValue={hierarchy}
                onChange={(value) => passStateToParent({ hierarchy: value })}
            />
        </>
    );
};
