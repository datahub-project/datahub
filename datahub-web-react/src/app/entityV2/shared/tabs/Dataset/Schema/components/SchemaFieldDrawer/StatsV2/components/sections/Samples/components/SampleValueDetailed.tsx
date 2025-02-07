import { borders, Button, colors, radius, spacing, Text } from '@src/alchemy-components';
import styled from 'styled-components';
import { message } from 'antd';
import React from 'react';

interface SampleValueDetailedProps {
    sample: string;
}

const Container = styled.div`
    border: ${borders['1px']} ${colors.gray[100]};
    padding: ${spacing.md};
    border-radius: ${radius.lg};
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: flex-start;
`;

const Sample = styled(Text)`
    text-wrap: auto;
`;

export default function SampleValueDetailed({ sample }: SampleValueDetailedProps) {
    const copySample = () => {
        navigator.clipboard.writeText(sample);
        message.success('Copied!');
    };

    return (
        <Container>
            <Sample type="pre" color="gray">
                {sample}
            </Sample>

            <Button
                icon="ContentCopy"
                iconPosition="left"
                isCircle
                onClick={copySample}
                size="xl"
                variant="text"
                color="gray"
            />
        </Container>
    );
}
