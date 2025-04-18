import React from 'react';
import styled from 'styled-components';

import { Constraint as ConstraintType } from '../../../types.generated';
import SatisfiedConstraint from './SatisfiedConstraint';
import UnsatisfiedConstraint from './UnsatisfiedConstraint';

type Props = {
    constraints?: ConstraintType[];
};

const ConstraintContainer = styled.div`
    margin-bottom: 8px;
`;

const SatisfiedConstraintsContainer = styled.div`
    display: flex;
    gap: 5px;
    flex-wrap: wrap;
    margin-bottom: 8px;
`;

export default function ConstraintGroup({ constraints }: Props) {
    const satisfiedConstraints = constraints?.filter((constraint) => constraint?.isSatisfied);
    const unsatisfiedConstraints = constraints?.filter((constraint) => !constraint?.isSatisfied);

    return (
        <ConstraintContainer>
            <SatisfiedConstraintsContainer>
                {satisfiedConstraints?.map((constraint) => (
                    <SatisfiedConstraint constraint={constraint} />
                ))}
            </SatisfiedConstraintsContainer>
            {unsatisfiedConstraints?.map((constraint) => (
                <UnsatisfiedConstraint constraint={constraint} />
            ))}
        </ConstraintContainer>
    );
}
