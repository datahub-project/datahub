import { PlusOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import { set, get } from 'lodash';
import YAML from 'yamljs';
import styled from 'styled-components/macro';
import StepButtons from '../StepButtons';
import { IngestionSourceBuilderStep } from '../steps';
import { SourceBuilderState, StepProps, Transformer } from '../types';
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
    margin: 10px 0 16px 0;
`;

function updateRecipeInState(
    state: SourceBuilderState,
    transformers: Transformer[],
    updateState: (newState: SourceBuilderState) => void,
) {
    const recipe = state.config?.recipe || '';
    const jsonRecipe = YAML.parse(recipe);
    const jsonTransformers = transformers
        .filter((t) => t.type)
        .map((transformer) => {
            return {
                type: transformer.type,
                config: {
                    urns: transformer.urns,
                },
            };
        });
    const transformersValue = jsonTransformers.length > 0 ? jsonTransformers : undefined;
    set(jsonRecipe, 'transformers', transformersValue);
    const jsonRecipeString = JSON.stringify(jsonRecipe);
    const newState = {
        ...state,
        config: {
            ...state.config,
            recipe: jsonRecipeString,
        },
    };
    updateState(newState);
}

function getInitialState(state: SourceBuilderState) {
    const jsonState = YAML.parse(state.config?.recipe || '');
    const jsonTransformers = get(jsonState, 'transformers') || [];
    return jsonTransformers.map((t) => {
        return { type: t.type, urns: t.config.urns || [] };
    });
}

export default function AddTransformersStep({ goTo, prev, state, updateState }: StepProps) {
    const [transformers, setTransformers] = useState<Transformer[]>(getInitialState(state));

    function onClickNext() {
        updateRecipeInState(state, transformers, updateState);
        goTo(IngestionSourceBuilderStep.CREATE_SCHEDULE);
    }

    function onClickPrevious() {
        updateRecipeInState(state, transformers, updateState);
        prev?.();
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
            {/* {transformers.length > 0 && <Divider style={{ margin: 0 }} />} */}
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
                onClickPrevious={onClickPrevious}
                onClickSkip={onClickNext}
                isNextDisabled={false}
            />
        </>
    );
}
