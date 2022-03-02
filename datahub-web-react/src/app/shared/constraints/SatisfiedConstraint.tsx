import React from 'react';
import { Tooltip } from 'antd';
import { CheckCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components';

import { Constraint as ConstraintType } from '../../../types.generated';

type Props = {
    constraint?: ConstraintType;
};

const SatisfiedConstraintPopoverContent = styled.div`
    align-items: center;
`;

export default function SatisfiedConstraint({ constraint }: Props) {
    return (
        <SatisfiedConstraintPopoverContent>
            <Tooltip title={constraint?.reason}>
                <CheckCircleOutlined style={{ color: 'green' }} />
            </Tooltip>
        </SatisfiedConstraintPopoverContent>
    );
}
