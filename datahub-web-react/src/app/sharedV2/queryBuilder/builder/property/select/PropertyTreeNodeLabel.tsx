/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CaretRightFilled } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';

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
