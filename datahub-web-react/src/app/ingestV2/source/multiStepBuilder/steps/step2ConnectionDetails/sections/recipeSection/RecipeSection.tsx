import { LegacyForm as Form, toast } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
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
    const { t } = useTranslation('ingestion.sourceBuilder');
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
                        ? t('recipeBuilder.fixLine', { line: (e as any).parsedLine })
                        : t('recipeBuilder.fixRecipe');
                    throw new Error(t('recipeBuilder.invalidYaml', { messageText }));
                }

                if (
                    parsedYaml &&
                    !!existingIngestionSource &&
                    parsedYaml?.source?.type !== existingIngestionSource.type
                ) {
                    throw new Error(t('multiStep.connection.cannotChangeSourceType'));
                }

                setSelectedTabKey(activeKey);
            } catch (e: unknown) {
                toast.destroy();
                if (e instanceof Error) {
                    toast.warning(e.message);
                }
            }
        },
        [displayRecipe, runFormValidation, existingIngestionSource, t],
    );

    const tabs: Tab[] = useMemo(
        () => [
            {
                key: 'form',
                name: t('recipeBuilder.formView'),
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
                name: t('recipeBuilder.yamlView'),
                component: <YamlEditor value={displayRecipe} onChange={setStagedRecipe} />,
                dataTestId: 'yaml-editor-tab',
            },
        ],
        [displayRecipe, state, sourceConfigs, setStagedRecipe, form, runFormValidation, t],
    );

    if (hasForm) {
        // destroyInactiveTabPane is required to reset state of RecipeForm with updated values from YAML editor
        return <Tabs tabs={tabs} selectedTab={selectedTabKey} onTabClick={onTabClick} destroyInactiveTabPane />;
    }

    return <YamlEditor value={displayRecipe} onChange={setStagedRecipe} />;
}
