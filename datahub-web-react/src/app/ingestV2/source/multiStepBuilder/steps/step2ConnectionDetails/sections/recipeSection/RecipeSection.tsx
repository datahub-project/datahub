import React, { useMemo } from 'react';
import styled from 'styled-components';

import { Tab, Tabs } from '@components/components/Tabs/Tabs';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { CONNECTORS_WITH_FORM } from '@app/ingestV2/source/builder/RecipeForm/constants';
import { YamlEditor } from '@app/ingestV2/source/builder/YamlEditor';
import { SourceConfig } from '@app/ingestV2/source/builder/types';
import RecipeForm from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/RecipeForm';
import { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';

const BorderedSection = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 16px;
    border: solid ${ANTD_GRAY[4]} 0.5px;
`;

interface Props {
    state: MultiStepSourceBuilderState;
    displayRecipe: string;
    sourceConfigs?: SourceConfig;
    setStagedRecipe: (recipe: string) => void;
}

export function RecipeSection({ state, displayRecipe, sourceConfigs, setStagedRecipe }: Props) {
    const { type } = state;
    const hasForm = useMemo(() => type && CONNECTORS_WITH_FORM.has(type), [type]);

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
                    />
                ),
            },
            {
                key: 'yaml',
                name: 'YAML',
                component: (
                    <BorderedSection>
                        <YamlEditor initialText={displayRecipe} onChange={setStagedRecipe} />
                    </BorderedSection>
                ),
            },
        ],
        [displayRecipe, state, sourceConfigs, setStagedRecipe],
    );

    if (hasForm) {
        // destroyInactiveTabPane is required to reset state of RecipeForm with updated values from YAML editor
        return <Tabs tabs={tabs} destroyInactiveTabPane />;
    }

    return (
        <BorderedSection>
            <YamlEditor initialText={displayRecipe} onChange={setStagedRecipe} />
        </BorderedSection>
    );
}
