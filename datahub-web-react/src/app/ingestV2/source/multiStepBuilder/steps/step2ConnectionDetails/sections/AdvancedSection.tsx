import { Input, spacing, transition } from '@components';
import { Form } from 'antd';
import useFormInstance from 'antd/lib/form/hooks/useFormInstance';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ExternalPluginKey, ExtraEnvKey, ExtraPluginKey, ExtraReqKey } from '@app/ingestV2/source/extraArgKeys';
import { AntdFormCompatibleCheckbox } from '@app/ingestV2/source/multiStepBuilder/components/AntdCompatibleCheckbox';
import { ExpandCollapseButton } from '@app/ingestV2/source/multiStepBuilder/components/ExpandCollapseButton';
import { SectionName } from '@app/ingestV2/source/multiStepBuilder/components/SectionName';
import { MAX_FORM_WIDTH } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/constants';
import { CustomLabelFormItem } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/CustomFormItem';
import { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.sm};
`;

const FormContainer = styled(Container)<{ $expanded?: boolean }>`
    max-width: ${MAX_FORM_WIDTH};
    max-height: ${(props) => (props.$expanded ? '100%' : '0px')};
    transform-origin: top;
    opacity: ${(props) => (props.$expanded ? 1 : 0)};
    transform: ${(props) => (props.$expanded ? 'scaleY(1)' : 'scaleY(0)')};
    transition:
        transform ${transition.duration.normal} ${transition.easing['ease-in-out']},
        opacity ${transition.duration.normal} ${transition.easing['ease-in-out']};
`;

interface Props {
    state: MultiStepSourceBuilderState;
    updateState: (newState: Partial<MultiStepSourceBuilderState>) => void;
}

const EXECUTOR_ID_PLACEHOLDER = 'default';
const CLI_VERSION_PLACEHOLDER = '(e.g. 0.15.0)';
const EXTRA_ENV_PLACEHOLDER = '{"MY_CUSTOM_ENV": "my_custom_value2"}';
const EXTRA_ARGS_PLACEHOLDER = '["debug"]';
const EXTRA_PIP_PLACEHOLDER = '["sqlparse==0.4.3"]';
const EXTERNAL_PLUGIN_PLACEHOLDER = '["github:owner/my-source"]';
export function AdvancedSection({ state, updateState }: Props) {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const [isExpanded, setIsExpanded] = useState<boolean>(false);
    const sectionRef = useRef<HTMLDivElement | null>(null);

    const toggleIsExpanded = useCallback(() => {
        setIsExpanded((prev) => !prev);
    }, []);

    function retrieveValueFromExtraArgs(builderState: MultiStepSourceBuilderState, key: string) {
        return builderState.config?.extraArgs?.find((entry) => entry.key === key)?.value ?? '';
    }

    const initialState = useMemo(() => {
        return {
            executor_id: state.config?.executorId ?? '',
            cli_version: state.config?.version ?? '',
            debug_mode: state.config?.debugMode ?? '',
            extra_args: retrieveValueFromExtraArgs(state, ExtraEnvKey),
            extra_pip_plugin: retrieveValueFromExtraArgs(state, ExtraPluginKey),
            extra_pip_reqs: retrieveValueFromExtraArgs(state, ExtraReqKey),
            external_plugins: retrieveValueFromExtraArgs(state, ExternalPluginKey),
        };
    }, [state]);

    const form = useFormInstance();

    const onValuesChange = useCallback(
        (_, values) => {
            updateState({
                ...state,
                config: {
                    ...state.config,
                    executorId: values.executor_id,
                    version: values.cli_version,
                    debugMode: values.debug_mode,
                    extraArgs: [
                        { key: ExtraEnvKey, value: values.extra_args },
                        { key: ExtraPluginKey, value: values.extra_pip_plugin },
                        { key: ExtraReqKey, value: values.extra_pip_reqs },
                        { key: ExternalPluginKey, value: values.external_plugins },
                    ],
                },
            });
        },
        [updateState, state],
    );

    useEffect(() => {
        if (isExpanded) {
            sectionRef.current?.scrollIntoView({
                behavior: 'smooth',
                block: 'start',
            });
        }
    }, [isExpanded]);

    return (
        <Form form={form} layout="vertical" initialValues={initialState} onValuesChange={onValuesChange}>
            <Container ref={sectionRef}>
                <SectionName
                    name={t('multiStep.connection.advancedSettings')}
                    topRowLeftItems={<ExpandCollapseButton expanded={isExpanded} />}
                    onHeaderClick={toggleIsExpanded}
                />

                <FormContainer $expanded={isExpanded}>
                    {/* NOTE: Executor ID is OSS-only, used by actions pod */}
                    <CustomLabelFormItem
                        label={t('multiStep.connection.executorId.label')}
                        help={t('multiStep.connection.executorId.help')}
                        name="executor_id"
                    >
                        <Input placeholder={EXECUTOR_ID_PLACEHOLDER} value={state.config?.executorId || ''} />
                    </CustomLabelFormItem>

                    <CustomLabelFormItem
                        label={t('multiStep.connection.cliVersion.label')}
                        help={t('multiStep.connection.cliVersion.help')}
                        name="cli_version"
                    >
                        <Input data-testid="cli-version-input" placeholder={CLI_VERSION_PLACEHOLDER} />
                    </CustomLabelFormItem>

                    <CustomLabelFormItem label={t('multiStep.connection.debugMode.label')} name="debug_mode">
                        <AntdFormCompatibleCheckbox
                            checked={state.config?.debugMode || false}
                            helper={t('multiStep.connection.debugMode.help')}
                        />
                    </CustomLabelFormItem>

                    <CustomLabelFormItem
                        label={t('multiStep.connection.extraEnvVars.label')}
                        help={t('multiStep.connection.extraEnvVars.help')}
                        name="extra_args"
                    >
                        <Input data-testid="extra-args-input" placeholder={EXTRA_ENV_PLACEHOLDER} />
                    </CustomLabelFormItem>

                    <CustomLabelFormItem
                        label={t('multiStep.connection.extraPlugins.label')}
                        help={t('multiStep.connection.extraPlugins.help')}
                        name="extra_pip_plugin"
                    >
                        <Input data-testid="extra-pip-plugin-input" placeholder={EXTRA_ARGS_PLACEHOLDER} />
                    </CustomLabelFormItem>

                    <CustomLabelFormItem
                        label={t('multiStep.connection.extraPipLibraries.label')}
                        help={t('multiStep.connection.extraPipLibraries.help')}
                        name="extra_pip_reqs"
                    >
                        <Input data-testid="extra-pip-reqs-input" placeholder={EXTRA_PIP_PLACEHOLDER} />
                    </CustomLabelFormItem>

                    <CustomLabelFormItem
                        label={t('multiStep.connection.externalPlugins.label')}
                        help={t('multiStep.connection.externalPlugins.help')}
                        name="external_plugins"
                    >
                        <Input data-testid="external-plugins-input" placeholder={EXTERNAL_PLUGIN_PLACEHOLDER} />
                    </CustomLabelFormItem>
                </FormContainer>
            </Container>
        </Form>
    );
}
