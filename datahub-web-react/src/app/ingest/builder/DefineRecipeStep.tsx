import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { IngestionSourceBuilderStep, RecipeBuilderState, StepProps } from './types';
import { defaultRecipe, jsonToYaml, yamlToJson } from './utils';
import { YamlEditor } from './YamlEditor';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const SelectTemplateHeader = styled(Typography.Title)`
    && {
        margin-bottom: 16px;
    }
`;

/**
 * The step for defining a recipe
 */
export const DefineRecipeStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const [stagedRecipeYml, setStagedRecipeYml] = useState('');
    const [stepComplete, setStepComplete] = useState(false);

    const existingRecipe = (state as RecipeBuilderState).recipe;
    const recipeToYaml = existingRecipe && jsonToYaml(existingRecipe);

    const validateRecipe = (recipeYml: string) => {
        console.log(recipeYml);
        return true;
    };

    const onClickNext = () => {
        // Validate the recipe yaml.
        validateRecipe(stagedRecipeYml);

        // Convert the recipe into it's json representation
        const recipeJson = yamlToJson(stagedRecipeYml);

        // Update the state
        const newState: RecipeBuilderState = {
            ...state,
            type: 'recipe',
            recipe: recipeJson,
        };
        updateState(newState);
        goTo(IngestionSourceBuilderStep.CREATE_SCHEDULE);
    };

    const handleChange = (value: any) => {
        setStagedRecipeYml(value);
        if (validateRecipe(value)) {
            setStepComplete(true);
        }
    };

    return (
        <>
            <Section>
                <SelectTemplateHeader level={5}>Define a Recipe</SelectTemplateHeader>
                <Typography.Text>
                    For information about how to define a recipe, see the{' '}
                    <a
                        href="https://datahubproject.io/docs/metadata-ingestion/"
                        target="_blank"
                        rel="noopener noreferrer"
                    >
                        Metadata Ingestion docs.
                    </a>
                </Typography.Text>
            </Section>
            <Section style={{ border: 'solid #D8D8D8 1px' }}>
                <YamlEditor initialText={recipeToYaml || defaultRecipe} onChange={handleChange} />
            </Section>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 8 }}>
                <Button onClick={() => prev()}>Previous</Button>
                <Button disabled={!stepComplete} onClick={onClickNext}>
                    Next
                </Button>
            </div>
        </>
    );
};
