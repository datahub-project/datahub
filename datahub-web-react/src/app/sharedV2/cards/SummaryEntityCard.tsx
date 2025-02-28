import React from 'react';
import styled from 'styled-components/macro';
import { EntityCard } from '../../homeV2/content/recent/EntityCard';
import { Entity } from '../../../types.generated';

const StyledEntityCard = styled(EntityCard)`
    min-width: 150px;
    max-width: 220px;
`;

interface Props {
    entity: Entity;
}

export default function SummaryEntityCard({ entity }: Props) {
    return <StyledEntityCard entity={entity} />;
}
