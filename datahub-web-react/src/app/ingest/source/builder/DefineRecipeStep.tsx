import { Alert, Button, message, Space, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { StepProps } from './types';
import { getSourceConfigs, jsonToYaml, yamlToJson } from '../utils';
import { YamlEditor } from './YamlEditor';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { IngestionSourceBuilderStep } from './steps';

const LOOKML_DOC_LINK = 'https://datahubproject.io/docs/generated/ingestion/sources/looker#module-lookml';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 16px;
`;

const BorderedSection = styled(Section)`
    border: solid ${ANTD_GRAY[4]} 0.5px;
`;

const SelectTemplateHeader = styled(Typography.Title)`
    && {
        margin-bottom: 8px;
    }
`;

const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

/**
 * The step for defining a recipe
 */
export const DefineRecipeStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const existingRecipeJson = state.config?.recipe;
    const existingRecipeYaml = existingRecipeJson && jsonToYaml(existingRecipeJson);

    const [stagedRecipeYml, setStagedRecipeYml] = useState(existingRecipeYaml || '');

    useEffect(() => {
        setStagedRecipeYml(existingRecipeYaml || '');
    }, [existingRecipeYaml]);

    const [stepComplete, setStepComplete] = useState(false);

    const { type } = state;
    const sourceConfigs = getSourceConfigs(type as string);
    const isEditing: boolean = prev === undefined;
    const displayRecipe = stagedRecipeYml || sourceConfigs.placeholderRecipe;
    const sourceDisplayName = sourceConfigs.displayName;
    const sourceDocumentationUrl = sourceConfigs.docsUrl; // Maybe undefined (in case of "custom")

    // TODO: Delete LookML banner specific code
    const isSourceLooker: boolean = sourceConfigs.type === 'looker';
    const [showLookerBanner, setShowLookerBanner] = useState(isSourceLooker && !isEditing);

    useEffect(() => {
        if (stagedRecipeYml && stagedRecipeYml.length > 0 && !showLookerBanner) {
            setStepComplete(true);
        }
    }, [stagedRecipeYml, showLookerBanner]);

    const onClickNext = () => {
        // Convert the recipe into it's json representation, and catch + report exceptions while we do it.
        let recipeJson;
        try {
            recipeJson = yamlToJson(stagedRecipeYml);
        } catch (e) {
            message.warn('Found invalid YAML. Please check your recipe configuration.');
            return;
        }

        const newState = {
            ...state,
            config: {
                ...state.config,
                recipe: recipeJson,
            },
        };
        updateState(newState);

        goTo(IngestionSourceBuilderStep.CREATE_SCHEDULE);
    };

    return (
        <>
            <Section>
                <SelectTemplateHeader level={5}>Configure {sourceDisplayName} Recipe</SelectTemplateHeader>
                {showLookerBanner && (
                    <Alert
                        type="warning"
                        banner
                        message={
                            <>
                                <big>
                                    <i>
                                        <b>You must acknowledge this message to proceed!</b>
                                    </i>
                                </big>
                                <br />
                                <br />
                                To get complete Looker metadata integration (including Looker views and lineage to the
                                underlying warehouse tables), you must <b>also</b> use the{' '}
                                <a href={LOOKML_DOC_LINK} target="_blank" rel="noopener noreferrer">
                                    DataHub lookml module
                                </a>
                                .
                                <br />
                                <br />
                                LookML ingestion <b>cannot</b> currently be performed via UI-based ingestion. This is a
                                known problem the DataHub team is working to solve!
                                <br />
                                <Space direction="horizontal" style={{ width: '100%', justifyContent: 'center' }}>
                                    <Button type="ghost" size="small" onClick={() => setShowLookerBanner(false)}>
                                        I have set up LookML ingestion!
                                    </Button>
                                </Space>
                            </>
                        }
                        afterClose={() => setShowLookerBanner(false)}
                    />
                )}
                <Typography.Text>
                    {showLookerBanner && <br />}
                    For more information about how to configure a recipe, see the{' '}
                    <a href={sourceDocumentationUrl} target="_blank" rel="noopener noreferrer">
                        {sourceDisplayName} source docs.
                    </a>
                </Typography.Text>
            </Section>
            <BorderedSection>
                <YamlEditor initialText={displayRecipe} onChange={setStagedRecipeYml} />
            </BorderedSection>
            <ControlsContainer>
                <Button disabled={isEditing} onClick={prev}>
                    Previous
                </Button>
                <Button disabled={!stepComplete} onClick={onClickNext}>
                    Next
                </Button>
            </ControlsContainer>
        </>
    );
};
