import { PlusOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import StepButtons from '../StepButtons';
import { IngestionSourceBuilderStep } from '../steps';
import { StepProps, Transformer } from '../types';
import TransformerInput from './TransformerInput';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 6px;
`;

const SelectTemplateHeader = styled(Typography.Title)`
    && {
        margin-bottom: 8px;
    }
`;

const AddTransformerButton = styled(Button)`
    margin: 16px 0;
`;

export default function AddTransformersStep({ goTo, prev }: StepProps) {
    const [transformers, setTransformers] = useState<Transformer[]>([]);

    function onClickNext() {
        goTo(IngestionSourceBuilderStep.CREATE_SCHEDULE);
    }

    function addNewTransformer() {
        setTransformers((prevTransformers) => [...prevTransformers, { type: null, urns: [] }]);
    }

    const existingTransformerTypes = transformers.map((transformer) => transformer.type);

    return (
        <>
            <Section>
                <SelectTemplateHeader level={5}>Add Metadata Transformers</SelectTemplateHeader>
                <Typography.Text>Optionally enrich your metadata while you are ingesting it.</Typography.Text>
            </Section>
            {transformers.map((transformer, index) => {
                return (
                    <TransformerInput
                        transformer={transformer}
                        existingTransformerTypes={existingTransformerTypes}
                        index={index}
                        setTransformers={setTransformers}
                    />
                );
            })}
            <AddTransformerButton onClick={addNewTransformer}>
                <PlusOutlined />
                Add Transformer
            </AddTransformerButton>
            <StepButtons
                onClickNext={onClickNext}
                onClickPrevious={prev}
                onClickSkip={onClickNext}
                isNextDisabled={false}
            />
        </>
    );
}
