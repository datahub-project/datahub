import { colors } from '@components';
import { Card } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { TestCardDetails } from '@app/tests/card/TestCardDetails';
import { TestCardResults } from '@app/tests/card/TestCardResults';

import { Test } from '@types';

const StyledCard = styled(Card)`
    border-radius: 12px;
    border: solid 1px ${colors.gray[100]};
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
    min-width: 138px;

    .ant-card-head {
        padding: 12px 12px; // Reduce padding (adjust values as needed)
    }

    // You might also want to adjust these related elements
    .ant-card-head-title {
        padding: 0px 0; // Reduced from default
    }

    .ant-card-extra {
        padding: 0px 0; // Reduced from default
    }
    .ant-card-body {
        padding: 12px 12px; // Reduce padding (adjust values as needed)
        display: flex;
        flex-wrap: wrap;
    }
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
