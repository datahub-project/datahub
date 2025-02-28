import React, { useState } from 'react';
import styled from 'styled-components';
import { Card } from 'antd';
import { TestCardDetails } from './TestCardDetails';
import { TestCardResults } from './TestCardResults';
import { Test } from '../../../types.generated';

const StyledCard = styled(Card)`
    margin: 8px 12px 12px 12px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
`;

type Props = {
    test: Test;
    onEdited?: (newTest) => void;
    onDeleted?: () => void;
    index: number;
};

export const TestCard = ({ test, onDeleted, index, ...props }: Props) => {
    const [editVersion, setEditVersion] = useState(0);

    const onEdited = (newTest) => {
        setEditVersion((currentVal: number) => currentVal + 1);
        props.onEdited?.(newTest);
    };

    return (
        <StyledCard title={<TestCardDetails test={test} index={index} onEdited={onEdited} onDeleted={onDeleted} />}>
            <TestCardResults urn={test.urn} name={test.name} editVersion={editVersion} />
        </StyledCard>
    );
};
