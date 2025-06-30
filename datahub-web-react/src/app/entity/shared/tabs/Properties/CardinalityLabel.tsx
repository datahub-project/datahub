import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { PropertyTypeBadge } from '@app/entity/shared/tabs/Dataset/Schema/components/PropertyTypeLabel';
import { getStructuredPropertyValue } from '@app/entity/shared/utils';

import { PropertyCardinality, StructuredPropertyEntity } from '@types';

const Header = styled.div`
    font-size: 10px;
`;

const List = styled.ul`
    padding: 0 24px;
    max-height: 500px;
    overflow: auto;
`;

interface Props {
    structuredProperty: StructuredPropertyEntity;
}

export default function CardinalityLabel({ structuredProperty }: Props) {
    const labelText =
        structuredProperty.definition.cardinality === PropertyCardinality.Single ? 'Single-Select' : 'Multi-Select';

    return (
        <Tooltip
            color="#373D44"
            title={
                <>
                    <Header>Property Options</Header>
                    <List>
                        {structuredProperty.definition.allowedValues?.map((value) => (
                            <li>{getStructuredPropertyValue(value.value)}</li>
                        ))}
                    </List>
                </>
            }
        >
            <PropertyTypeBadge count={labelText} />
        </Tooltip>
    );
}
