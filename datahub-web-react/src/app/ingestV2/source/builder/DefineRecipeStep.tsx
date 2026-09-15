import { Alert, Space, Typography, message } from 'antd';
import React, { useEffect, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import RecipeBuilder from '@app/ingestV2/source/builder/RecipeBuilder';
import { getRecipeJson } from '@app/ingestV2/source/builder/RecipeForm/TestConnection/TestConnectionButton';
import { CONNECTORS_WITH_FORM_NO_DYNAMIC_FIELDS } from '@app/ingestV2/source/builder/RecipeForm/constants';
import { YamlEditor } from '@app/ingestV2/source/builder/YamlEditor';
import { IngestionSourceBuilderStep } from '@app/ingestV2/source/builder/steps';
import { StepProps } from '@app/ingestV2/source/builder/types';
import {
    CUSTOM_SOURCE_DISPLAY_NAME,
    getPlaceholderRecipe,
    getSourceConfigs,
    jsonToYaml,
} from '@app/ingestV2/source/utils';
import { Button } from '@src/alchemy-components';

const LOOKML_DOC_LINK = 'https://docs.datahub.com/docs/generated/ingestion/sources/looker#module-lookml';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 16px;
`;

const BorderedSection = styled(Section)`
    border: solid ${(props) => props.theme.colors.border} 0.5px;
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
export const DefineRecipeStep = ({
    state,
    updateState,
    goTo,
    prev,
    ingestionSources,
    selectedSource,
    setSelectedSourceType,
}: StepProps) => {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const { t: tc } = useTranslation('common.actions');
    const existingRecipeJson = state.config?.recipe;
    const existingRecipeYaml = existingRecipeJson && jsonToYaml(existingRecipeJson);
    const { type } = state;
    const sourceConfigs = getSourceConfigs(ingestionSources, type as string);
    const placeholderRecipe = getPlaceholderRecipe(ingestionSources, type);

    const [stagedRecipeYml, setStagedRecipeYml] = useState(existingRecipeYaml || placeholderRecipe);
    const [stagedRecipeName, setStagedRecipeName] = useState(state.name);

    useEffect(() => {
        if (existingRecipeYaml) {
            setStagedRecipeName(state.name);
            setStagedRecipeYml(existingRecipeYaml);
        }
    }, [existingRecipeYaml, state.name]);

    const [stepComplete, setStepComplete] = useState(false);

    const isEditing: boolean = prev === undefined;
    const displayRecipe = stagedRecipeYml || placeholderRecipe;
    const sourceDisplayName =
        sourceConfigs?.displayName === CUSTOM_SOURCE_DISPLAY_NAME ? '' : sourceConfigs?.displayName;
    const sourceDocumentationUrl = sourceConfigs?.docsUrl;

    // TODO: Delete LookML banner specific code
    const isSourceLooker: boolean = sourceConfigs?.name === 'looker';
    const [showLookerBanner, setShowLookerBanner] = useState(isSourceLooker && !isEditing);

    useEffect(() => {
        if (stagedRecipeYml && stagedRecipeYml.length > 0 && !showLookerBanner) {
            setStepComplete(true);
        }
    }, [stagedRecipeYml, showLookerBanner]);

    const onClickNext = () => {
        const recipeJson = getRecipeJson(stagedRecipeYml);
        if (!recipeJson) return;

        if (!JSON.parse(recipeJson).source?.type) {
            message.warning({
                content: t('defineRecipe.invalidIngestionType'),
                duration: 3,
            });
            return;
        }

        const newState = {
            ...state,
            config: {
                ...state.config,
                recipe: recipeJson,
            },
            type: JSON.parse(recipeJson).source.type,
        };
        updateState(newState);

        goTo(IngestionSourceBuilderStep.CREATE_SCHEDULE);
        setSelectedSourceType?.(newState.type);
    };

    if (type && CONNECTORS_WITH_FORM_NO_DYNAMIC_FIELDS.has(type)) {
        return (
            <RecipeBuilder
                key={stagedRecipeName}
                state={state}
                isEditing={isEditing}
                displayRecipe={displayRecipe}
                sourceConfigs={sourceConfigs}
                setStagedRecipe={setStagedRecipeYml}
                onClickNext={onClickNext}
                goToPrevious={prev}
                selectedSource={selectedSource}
            />
        );
    }

    return (
        <>
            <Section>
                <SelectTemplateHeader level={5}>
                    {t('defineRecipe.title', { displayName: sourceDisplayName })}
                </SelectTemplateHeader>
                {showLookerBanner && (
                    <Alert
                        type="warning"
                        banner
                        message={
                            <>
                                <big>
                                    <i>
                                        <b>{t('defineRecipe.lookerBanner.acknowledge')}</b>
                                    </i>
                                </big>
                                <br />
                                <br />
                                <Trans
                                    t={t}
                                    i18nKey="defineRecipe.lookerBanner.integration"
                                    components={{
                                        bold: <b />,
                                        anchor: (
                                            <a href={LOOKML_DOC_LINK} target="_blank" rel="noopener noreferrer">
                                                {t('defineRecipe.lookerBanner.lookmlModuleLinkText')}
                                            </a>
                                        ),
                                    }}
                                />
                                <br />
                                <br />
                                <Trans
                                    t={t}
                                    i18nKey="defineRecipe.lookerBanner.uiUnsupported"
                                    components={{ bold: <b /> }}
                                />
                                <br />
                                <Space direction="horizontal" style={{ width: '100%', justifyContent: 'center' }}>
                                    <Button variant="text" onClick={() => setShowLookerBanner(false)}>
                                        {t('defineRecipe.lookerBanner.acknowledgeButton')}
                                    </Button>
                                </Space>
                            </>
                        }
                        afterClose={() => setShowLookerBanner(false)}
                    />
                )}
                <Typography.Text>
                    {showLookerBanner && <br />}
                    <Trans
                        t={t}
                        i18nKey="defineRecipe.docsHint"
                        values={{ displayName: sourceDisplayName }}
                        components={{
                            anchor: (
                                <a href={sourceDocumentationUrl} target="_blank" rel="noopener noreferrer">
                                    {t('defineRecipe.docsLinkText', { displayName: sourceDisplayName })}
                                </a>
                            ),
                        }}
                    />
                </Typography.Text>
            </Section>
            <BorderedSection>
                <YamlEditor initialText={displayRecipe} onChange={setStagedRecipeYml} />
            </BorderedSection>
            <ControlsContainer>
                <Button variant="outline" color="gray" disabled={isEditing} onClick={prev}>
                    {tc('previous')}
                </Button>
                <Button disabled={!stepComplete} onClick={onClickNext} data-testid="recipe-builder-next-button">
                    {tc('next')}
                </Button>
            </ControlsContainer>
        </>
    );
};
