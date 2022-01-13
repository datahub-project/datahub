import React from 'react';
import styled from 'styled-components';

import { Constraint as ConstraintType } from '../../../types.generated';
import Constraint from './Constraint';

type Props = {
    constraints?: ConstraintType[];
};

const ConstraintContainer = styled.div`
    margin-bottom: 8px;
`;

export default function ConstraintGroup({ constraints }: Props) {
    return (
        <ConstraintContainer>
            {constraints?.map((constraint) => (
                <Constraint constraint={constraint} />
            ))}
        </ConstraintContainer>
    );
}
