import { Form, message } from 'antd';
import React, { useCallback, useMemo, useState } from 'react';
import YAML from 'yamljs';

import { Tab, Tabs } from '@components/components/Tabs/Tabs';

import { CONNECTORS_WITH_FORM_INCLUDING_DYNAMIC_FIELDS } from '@app/ingestV2/source/builder/RecipeForm/constants';
import { SourceConfig } from '@app/ingestV2/source/builder/types';
import { YamlEditor } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/YamlEditor';
import RecipeForm from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/RecipeForm';
import { IngestionSourceFormStep, MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

interface Props {
    state: MultiStepSourceBuilderState;
    displayRecipe: string;
    sourceConfigs?: SourceConfig;
    setStagedRecipe: (recipe: string) => void;
}

export function RecipeSection({ state, displayRecipe, sourceConfigs, setStagedRecipe }: Props) {
    const { type } = state;
    const hasForm = useMemo(() => type && CONNECTORS_WITH_FORM_INCLUDING_DYNAMIC_FIELDS.has(type), [type]);
    const [selectedTabKey, setSelectedTabKey] = useState<string>('form');
    const {
        state: { ingestionSource: existingIngestionSource },
    } = useMultiStepContext<MultiStepSourceBuilderState, IngestionSourceFormStep>();

    const [form] = Form.useForm();
    const runFormValidation = useCallback(() => {
        form.validateFields();
    }, [form]);

    const onTabClick = useCallback(
        (activeKey) => {
            if (activeKey !== 'form') {
                setSelectedTabKey(activeKey);
                return;
            }

            let parsedYaml: Record<string, any> | null = null;
            // Validate yaml content when switching from yaml tab to form
            try {
                try {
                    parsedYaml = YAML.parse(displayRecipe);
                    setTimeout(runFormValidation, 0); // let form remount and then run validation
                } catch (e) {
                    const messageText = (e as any).parsedLine
                        ? `Fix line ${(e as any).parsedLine} in your recipe`
                        : 'Please fix your recipe';
                    throw new Error(`Found invalid YAML. ${messageText} in order to switch views.`);
                }

                if (
                    parsedYaml &&
                    !!existingIngestionSource &&
                    parsedYaml?.source?.config?.type !== existingIngestionSource.type
                ) {
                    throw new Error("It's not possible to change source type for existing ingestion source");
                }

                setSelectedTabKey(activeKey);
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.warn(e.message);
                }
            }
        },
        [displayRecipe, runFormValidation, existingIngestionSource],
    );

    const tabs: Tab[] = useMemo(
        () => [
            {
                key: 'form',
                name: 'Form',
                component: (
                    <RecipeForm
                        state={state}
                        form={form}
                        runFormValidation={runFormValidation}
                        displayRecipe={displayRecipe}
                        sourceConfigs={sourceConfigs}
                        setStagedRecipe={setStagedRecipe}
                    />
                ),
            },
            {
                key: 'yaml',
                name: 'YAML',
                component: <YamlEditor value={displayRecipe} onChange={setStagedRecipe} />,
                dataTestId: 'yaml-editor-tab',
            },
        ],
        [displayRecipe, state, sourceConfigs, setStagedRecipe, form, runFormValidation],
    );

    if (hasForm) {
        // destroyInactiveTabPane is required to reset state of RecipeForm with updated values from YAML editor
        return <Tabs tabs={tabs} selectedTab={selectedTabKey} onTabClick={onTabClick} destroyInactiveTabPane />;
    }

    return <YamlEditor value={displayRecipe} onChange={setStagedRecipe} />;
}
