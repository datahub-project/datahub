import { TreeSelect, Typography } from 'antd';
import { TreeNode } from 'antd/lib/tree-select';
import React from 'react';
import styled from 'styled-components';

import { PropertyTreeNodeLabel } from '@app/sharedV2/queryBuilder/builder/property/select/PropertyTreeNodeLabel';
import { PropertyTreeNodeTitle } from '@app/sharedV2/queryBuilder/builder/property/select/PropertyTreeNodeTitle';
import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { VALUE_TYPE_ID_TO_DETAILS } from '@app/sharedV2/queryBuilder/builder/property/types/values';

const CUSTOM_ID = 'custom';

const StyledTreeSelect = styled(TreeSelect)`
    max-width: 400px;
    min-width: 240px;
    margin-right: 8px;
`;

type Props = {
    selectedProperty?: string;
    properties: Property[];
    onChangeProperty: (newPropertyId: string) => void;
};

/**
 * A dropdown select for well-supported, typed properties.
 */
export const PropertyTreeSelect = ({ selectedProperty, properties, onChangeProperty }: Props) => {
    /**
     * Render a single node in the property tree select
     */
    const renderNode = (property, parents) => {
        const valueTypeDetails = property.valueType && VALUE_TYPE_ID_TO_DETAILS.get(property.valueType);
        const valueTypeDisplayName = valueTypeDetails && valueTypeDetails.displayName;
        const selectable = !!property.valueType; // If a prop has a value type, we can select it.

        return (
            <TreeNode
                selectable={selectable}
                label={<PropertyTreeNodeLabel property={property} parents={parents} valueType={valueTypeDisplayName} />}
                value={property.id}
                title={
                    <PropertyTreeNodeTitle
                        property={property}
                        selectable={!!selectable}
                        valueType={valueTypeDisplayName}
                    />
                }
                key={property.id}
            >
                {property.children?.map((child) => renderNode(child, [...parents, property]))}
            </TreeNode>
        );
    };

    return (
        <StyledTreeSelect
            autoFocus
            treeNodeLabelProp="label"
            placeholder="Select a property..."
            onSelect={(newVal) => onChangeProperty(newVal as string)}
            value={selectedProperty}
            treeDefaultExpandAll
            treeExpandAction="click"
        >
            {properties.map((property) => {
                return renderNode(property, []);
            })}
            <TreeNode
                key={CUSTOM_ID}
                label={<Typography.Text>Custom</Typography.Text>}
                value={CUSTOM_ID}
                title={<Typography.Text>Custom</Typography.Text>}
            />
        </StyledTreeSelect>
    );
};
