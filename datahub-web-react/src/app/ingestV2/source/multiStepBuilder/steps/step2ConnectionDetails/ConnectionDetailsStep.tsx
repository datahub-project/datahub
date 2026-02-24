import { message } from 'antd';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { ActorEntity } from '@app/entityV2/shared/utils/actorUtils';
import { CSVInfo } from '@app/ingestV2/source/builder/CSVInfo';
import { LookerWarning } from '@app/ingestV2/source/builder/LookerWarning';
import { getRecipeJson } from '@app/ingestV2/source/builder/RecipeForm/TestConnection/TestConnectionButton';
import { CSV, LOOKER, LOOK_ML } from '@app/ingestV2/source/builder/constants';
import { useIngestionSources } from '@app/ingestV2/source/builder/useIngestionSources';
import {
    INGESTION_TYPE_CHANGED_ERROR,
    INGESTION_TYPE_EMPTY_ERROR,
} from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/constants';
import { AdvancedSection } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/AdvancedSection';
import { NameAndOwnersSection } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/NameAndOwnersSection';
import { RecipeSection } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/RecipeSection';
import { IngestionSourceFormStep, MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { getPlaceholderRecipe, getSourceConfigs, jsonToYaml } from '@app/ingestV2/source/utils';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

export function ConnectionDetailsStep() {
    const { state, updateState, setCurrentStepCompleted, setCurrentStepUncompleted, setOnNextHandler } =
        useMultiStepContext<MultiStepSourceBuilderState, IngestionSourceFormStep>();

    const { ingestionSources } = useIngestionSources();

    const existingRecipeJson = state.ingestionSource?.config?.recipe;
    const existingRecipeYaml = existingRecipeJson && jsonToYaml(existingRecipeJson);
    const existingRecipeFromStateJson = state.config?.recipe;
    const existingRecipeFromStateYaml = existingRecipeFromStateJson && jsonToYaml(existingRecipeFromStateJson);
    const { type } = state;
    const isEditing = !!state.isEditing;
    const sourceConfigs = getSourceConfigs(ingestionSources, type as string);
    const placeholderRecipe = getPlaceholderRecipe(ingestionSources, type);
    const [initialRecipeYml] = useState(existingRecipeFromStateYaml || existingRecipeYaml);
    const [stagedRecipeYml, setStagedRecipeYml] = useState(initialRecipeYml || placeholderRecipe);

    const analyticsRef = useRef(false);

    const updateRecipe = useCallback(
        (recipe: string, shouldSetIsRecipeValid?: boolean, hideYamlWarnings = false) => {
            const recipeJson = getRecipeJson(recipe, hideYamlWarnings);
            if (!recipeJson) {
                throw Error('Invalid YAML');
            }

            const sourceType = JSON.parse(recipeJson).source?.type;

            if (!sourceType) {
                throw Error(INGESTION_TYPE_EMPTY_ERROR);
            }

            if (!!state.ingestionSource && sourceType !== state.ingestionSource.type) {
                throw Error(INGESTION_TYPE_CHANGED_ERROR);
            }

            const newState = {
                ...state,
                config: {
                    ...state.config,
                    recipe: recipeJson,
                },
                type: sourceType,
                ...(shouldSetIsRecipeValid ? { isRecipeValid: true } : {}),
            };
            updateState(newState);
        },
        [updateState, state],
    );

    const updateStagedRecipeAndState = useCallback(
        (recipe: string) => {
            setStagedRecipeYml(recipe);
            try {
                updateRecipe(recipe, false, true);
            } catch (e: unknown) {
                if (e instanceof Error) {
                    console.error(e.message);
                }
            }
        },
        [updateRecipe],
    );

    useEffect(() => {
        if (existingRecipeYaml) {
            setStagedRecipeYml(existingRecipeYaml);
        }
    }, [existingRecipeYaml, state.name]);

    const displayRecipe = stagedRecipeYml || placeholderRecipe;

    useEffect(() => {
        if (state.name) {
            setCurrentStepCompleted();
            updateState({ isConnectionDetailsValid: true });
        } else {
            setCurrentStepUncompleted();
            updateState({ isConnectionDetailsValid: false });
        }
    }, [updateState, setCurrentStepCompleted, setCurrentStepUncompleted, state.name]);

    useEffect(() => {
        if (analyticsRef.current) return;
        if (state) {
            analyticsRef.current = true;
            analytics.event({
                type: EventType.IngestionEnterConfigurationEvent,
                sourceType: state.type || '',
                sourceUrn: state.ingestionSource?.urn,
                configurationType: state.isEditing ? 'edit_existing' : 'create_new',
            });
        }
    }, [state]);

    const sourceName = useMemo(() => state.name || '', [state.name]);
    const updateSourceName = useCallback(
        (newSourceName: string) => updateState({ name: newSourceName }),
        [updateState],
    );

    const ownerUrns = useMemo(() => state.owners?.map((owner) => owner.urn) ?? [], [state.owners]);
    const updateOwners = useCallback((newOwners: ActorEntity[]) => updateState({ owners: newOwners }), [updateState]);

    const onNextHandler = useCallback(() => {
        try {
            updateRecipe(stagedRecipeYml, true);
        } catch (e: unknown) {
            if (e instanceof Error) {
                if (e.message === INGESTION_TYPE_EMPTY_ERROR) {
                    message.warning({
                        content: 'Please add valid ingestion type',
                        duration: 3,
                    });
                } else if (e.message === INGESTION_TYPE_CHANGED_ERROR) {
                    message.warning({
                        content: "It's not possible to change source type for existing ingestion source",
                        duration: 3,
                    });
                }
            }
            throw e;
        }
    }, [stagedRecipeYml, updateRecipe]);

    useEffect(() => {
        setOnNextHandler(() => onNextHandler);
        return () => setOnNextHandler(undefined);
    }, [onNextHandler, setOnNextHandler]);

    return (
        <>
            {(type === LOOKER || type === LOOK_ML) && <LookerWarning type={type} />}
            {type === CSV && <CSVInfo />}
            <Container>
                <NameAndOwnersSection
                    source={state.ingestionSource}
                    sourceName={sourceName}
                    updateSourceName={updateSourceName}
                    ownerUrns={ownerUrns}
                    updateOwners={updateOwners}
                    isEditing={isEditing}
                />

                <RecipeSection
                    state={state}
                    displayRecipe={displayRecipe}
                    sourceConfigs={sourceConfigs}
                    setStagedRecipe={updateStagedRecipeAndState}
                />

                <AdvancedSection state={state} updateState={updateState} />
            </Container>
        </>
    );
}
