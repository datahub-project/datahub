import { EntityLink } from '@src/app/homeV2/reference/sections/EntityLink';
import { Entity } from '@src/types.generated';
import React from 'react';
import styled from 'styled-components';
import { LabelText } from './styledComponents';

const PropertyContainer = styled.div`
    display: flex;
    gap: 5px;
    margin-bottom: 6px;
    align-items: center; /* This will vertically center all items by default */

    // when this overflows, the entity link components should truncate text
    white-space: nowrap;
`;

interface Props {
    labelText: string;
    entity?: Entity | null;
}

const EntityProperty = ({ labelText, entity }: Props) => {
    return (
        <PropertyContainer>
            <LabelText>{labelText}</LabelText>
            {!!entity && <EntityLink entity={entity} />}
        </PropertyContainer>
    );
};

export default EntityProperty;
