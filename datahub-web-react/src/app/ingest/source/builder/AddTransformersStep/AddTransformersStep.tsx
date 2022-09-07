import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import StepButtons from '../StepButtons';
import { IngestionSourceBuilderStep } from '../steps';
import { StepProps } from '../types';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 16px;
    padding-top: 0px;
`;

const SelectTemplateHeader = styled(Typography.Title)`
    && {
        margin-bottom: 8px;
    }
`;

export default function AddTransformersStep({ goTo, prev }: StepProps) {
    function onClickNext() {
        goTo(IngestionSourceBuilderStep.CREATE_SCHEDULE);
    }

    return (
        <>
            <Section>
                <SelectTemplateHeader level={5}>Add Metadata Transformers</SelectTemplateHeader>
                <Typography.Text>Optionally enrich your metadata while you are ingesting it.</Typography.Text>
            </Section>
            <StepButtons
                onClickNext={onClickNext}
                onClickPrevious={prev}
                onClickSkip={onClickNext}
                isNextDisabled={false}
            />
        </>
    );
}
