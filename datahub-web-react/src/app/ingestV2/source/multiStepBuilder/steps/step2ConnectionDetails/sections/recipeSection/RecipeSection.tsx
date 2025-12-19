import { message } from 'antd';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import YAML from 'yamljs';

import { Tab, Tabs } from '@components/components/Tabs/Tabs';

import { CONNECTORS_WITH_FORM } from '@app/ingestV2/source/builder/RecipeForm/constants';
import { YamlEditor } from '@app/ingestV2/source/builder/YamlEditor';
import { SourceConfig } from '@app/ingestV2/source/builder/types';
import RecipeForm from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/RecipeForm';
import { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';

const EditorWrapper = styled.div`
    // overriding of datahub font with the default one for the editor
    && .view-line > span > span {
        font-family: 'Droid Sans Mono', 'monospace', monospace, 'Droid Sans Fallback' !important;
    }

    padding-bottom: 8px;
`;

interface Props {
    state: MultiStepSourceBuilderState;
    displayRecipe: string;
    sourceConfigs?: SourceConfig;
    setStagedRecipe: (recipe: string) => void;
    setIsRecipeValid?: (isValid: boolean) => void;
}

export function RecipeSection({ state, displayRecipe, sourceConfigs, setStagedRecipe, setIsRecipeValid }: Props) {
    const { type } = state;
    const isEditing = !!state.isEditing;
    const hasForm = useMemo(() => type && CONNECTORS_WITH_FORM.has(type), [type]);
    const [selectedTabKey, setSelectedTabKey] = useState<string>('form');
    // FYI: We don't have form validation for sources without a form
    useEffect(() => {
        setIsRecipeValid?.(!hasForm || isEditing || !!state.isConnectionDetailsValid);
    }, [hasForm, isEditing, setIsRecipeValid, state.isConnectionDetailsValid]);

    const onTabClick = useCallback(
        (activeKey) => {
            if (activeKey !== 'form') {
                setSelectedTabKey(activeKey);
                return;
            }

            // Validate yaml content when switching from yaml tab to form
            try {
                YAML.parse(displayRecipe);
                setSelectedTabKey(activeKey);
            } catch (e) {
                message.destroy();
                const messageText = (e as any).parsedLine
                    ? `Fix line ${(e as any).parsedLine} in your recipe`
                    : 'Please fix your recipe';
                message.warn(`Found invalid YAML. ${messageText} in order to switch views.`);
            }
        },
        [displayRecipe],
    );

    const tabs: Tab[] = useMemo(
        () => [
            {
                key: 'form',
                name: 'Form',
                component: (
                    <RecipeForm
                        state={state}
                        displayRecipe={displayRecipe}
                        sourceConfigs={sourceConfigs}
                        setStagedRecipe={setStagedRecipe}
                        setIsRecipeValid={setIsRecipeValid}
                    />
                ),
            },
            {
                key: 'yaml',
                name: 'YAML',
                component: (
                    <EditorWrapper>
                        <YamlEditor initialText={displayRecipe} onChange={setStagedRecipe} />
                    </EditorWrapper>
                ),
            },
        ],
        [displayRecipe, state, sourceConfigs, setStagedRecipe, setIsRecipeValid],
    );

    if (hasForm) {
        // destroyInactiveTabPane is required to reset state of RecipeForm with updated values from YAML editor
        return <Tabs tabs={tabs} selectedTab={selectedTabKey} onTabClick={onTabClick} destroyInactiveTabPane />;
    }

    return (
        <EditorWrapper>
            <YamlEditor initialText={displayRecipe} onChange={setStagedRecipe} />
        </EditorWrapper>
    );
}
