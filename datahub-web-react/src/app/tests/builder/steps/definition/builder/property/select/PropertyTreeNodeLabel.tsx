import React from 'react';
import styled from 'styled-components';
import { CaretRightFilled } from '@ant-design/icons';
import { Typography } from 'antd';
import { ANTD_GRAY } from '../../../../../../../entity/shared/constants';
import { Property } from '../types/properties';

const StyledCaret = styled(CaretRightFilled)`
    padding: 4px;
    color: ${ANTD_GRAY[5]};
    font-size: 12px;
`;

const ValueTypeLabel = styled(Typography.Text)`
    margin-left: 8px;
`;

type Props = {
    property: Property;
    parents: Property[];
    valueType?: string;
};

/**
 * A label displayed for a Property tree Node.
 *
 * This is shown in the select when a node is chosen.
 */
export const PropertyTreeNodeLabel = ({ property, parents, valueType }: Props) => {
    return (
        <>
            {parents.map((parentProp) => (
                <Typography.Text key={parentProp.id}>
                    {parentProp.displayName}
                    <StyledCaret />
                </Typography.Text>
            ))}
            <Typography.Text>{property.displayName}</Typography.Text>
            <ValueTypeLabel type="secondary">{valueType}</ValueTypeLabel>
        </>
    );
};
