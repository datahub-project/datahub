/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ControlOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import { SchemaField } from '@types';

const ColumnWrapper = styled.div`
    font-size: 14px;
`;

const StyledIcon = styled(ControlOutlined)`
    margin-right: 4px;
`;

interface Props {
    field: SchemaField;
}

export default function PropertiesColumn({ field }: Props) {
    const { schemaFieldEntity } = field;
    const numProperties = schemaFieldEntity?.structuredProperties?.properties?.filter(
        (prop) => !prop.structuredProperty.settings?.isHidden,
    )?.length;

    if (!schemaFieldEntity || !numProperties) return null;

    return (
        <ColumnWrapper>
            <StyledIcon />
            {numProperties} {numProperties === 1 ? 'property' : 'properties'}
        </ColumnWrapper>
    );
}
