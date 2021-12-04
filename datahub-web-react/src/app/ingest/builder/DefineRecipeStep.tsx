import { Button, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { IngestionSourceBuilderStep, StepProps } from './types';
import { getSourceConfigs, jsonToYaml, yamlToJson } from './utils';
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
    const existingRecipeJson = state.config?.recipe;
    const existingRecipeYaml = existingRecipeJson && jsonToYaml(existingRecipeJson);

    const [stagedRecipeYml, setStagedRecipeYml] = useState(existingRecipeYaml || '');
    const [stepComplete, setStepComplete] = useState(false);

    const { type } = state;
    const sourceConfigs = getSourceConfigs(type as string);
    const displayRecipe = stagedRecipeYml || sourceConfigs.recipe;
    const sourceDisplayName = sourceConfigs.displayName;
    const sourceDocumentationUrl = sourceConfigs.docsUrl; // Maybe undefined (in case of "custom")

    useEffect(() => {
        if (stagedRecipeYml && stagedRecipeYml.length > 0) {
            setStepComplete(true);
        }
    }, [stagedRecipeYml]);

    const onClickNext = () => {
        // Convert the recipe into it's json representation
        const recipeJson = yamlToJson(stagedRecipeYml);

        // Update the state
        const newState = {
            ...state,
            config: {
                recipe: recipeJson,
            },
        };
        updateState(newState);

        goTo(IngestionSourceBuilderStep.CREATE_SCHEDULE);
    };

    return (
        <>
            <Section>
                <SelectTemplateHeader level={5}>Create a {sourceDisplayName} recipe</SelectTemplateHeader>
                <Typography.Text>
                    For more information about how to define a {sourceDisplayName} recipe, see the{' '}
                    <a href={sourceDocumentationUrl} target="_blank" rel="noopener noreferrer">
                        source docs.
                    </a>
                </Typography.Text>
            </Section>
            <Section style={{ border: 'solid #D8D8D8 0.5px' }}>
                <YamlEditor initialText={displayRecipe} onChange={setStagedRecipeYml} />
            </Section>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 8 }}>
                <Button disabled={prev === undefined} onClick={prev}>
                    Previous
                </Button>
                <Button disabled={!stepComplete} onClick={onClickNext}>
                    Next
                </Button>
            </div>
        </>
    );
};
