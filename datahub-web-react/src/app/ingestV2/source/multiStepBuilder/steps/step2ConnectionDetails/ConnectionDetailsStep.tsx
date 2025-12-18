import { message } from 'antd';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { ActorEntity } from '@app/entityV2/shared/utils/actorUtils';
import { CSVInfo } from '@app/ingestV2/source/builder/CSVInfo';
import { LookerWarning } from '@app/ingestV2/source/builder/LookerWarning';
import { getRecipeJson } from '@app/ingestV2/source/builder/RecipeForm/TestConnection/TestConnectionButton';
import { CSV, LOOKER, LOOK_ML } from '@app/ingestV2/source/builder/constants';
import { useIngestionSources } from '@app/ingestV2/source/builder/useIngestionSources';
import { AdvancedSection } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/AdvansedSection';
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
    const [isRecipeValid, setIsRecipeValid] = useState<boolean>(isEditing || !!state.isConnectionDetailsValid);
    const [initialRecipeYml] = useState(existingRecipeFromStateYaml || existingRecipeYaml);
    const [stagedRecipeYml, setStagedRecipeYml] = useState(initialRecipeYml || placeholderRecipe);

    const updateRecipe = useCallback(
        (recipe: string, shouldSetIsRecipeValid?: boolean) => {
            const recipeJson = getRecipeJson(recipe);
            if (!recipeJson) return;

            if (!JSON.parse(recipeJson).source?.type) {
                throw Error('Ingestion type is undefined');
            }

            const newState = {
                ...state,
                config: {
                    ...state.config,
                    recipe: recipeJson,
                },
                type: JSON.parse(recipeJson).source.type,
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
                updateRecipe(recipe);
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
        if (isRecipeValid && state.name && stagedRecipeYml && stagedRecipeYml.length > 0) {
            setCurrentStepCompleted();
            updateState({ isConnectionDetailsValid: true });
        } else {
            setCurrentStepUncompleted();
            updateState({ isConnectionDetailsValid: false });
        }
    }, [isRecipeValid, updateState, stagedRecipeYml, setCurrentStepCompleted, setCurrentStepUncompleted, state.name]);

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
                message.warning({
                    content: `Please add valid ingestion type`,
                    duration: 3,
                });
            }
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
                    setIsRecipeValid={setIsRecipeValid}
                />

                <AdvancedSection state={state} updateState={updateState} />
            </Container>
        </>
    );
}
