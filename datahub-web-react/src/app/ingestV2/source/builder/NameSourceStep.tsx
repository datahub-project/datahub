import { Button, Text, Tooltip } from '@components';
import { Checkbox, Collapse, Form, Input, Typography } from 'antd';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';
import { ActorEntity } from '@app/entityV2/shared/utils/actorUtils';
import { SourceBuilderState, StepProps, StringMapEntryInput } from '@app/ingestV2/source/builder/types';
import { RequiredFieldForm } from '@app/shared/form/RequiredFieldForm';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';

import { Entity, Owner } from '@types';

const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

const LabelContainer = styled.div`
    display: flex;
`;

const ExtraEnvKey = 'extra_env_vars';
const ExtraReqKey = 'extra_pip_requirements';
const ExtraPluginKey = 'extra_pip_plugins';

const EXECUTOR_ID_PLACEHOLDER = 'default';
const EXTRA_ENV_PLACEHOLDER = '{"MY_CUSTOM_ENV": "my_custom_value2"}';
const EXTRA_ARGS_PLACEHOLDER = '["debug"]';
const EXTRA_PIP_PLACEHOLDER = '["sqlparse==0.4.3"]';

export const NameSourceStep = ({ state, updateState, prev, submit, isEditing, selectedSource }: StepProps) => {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const { t: tc } = useTranslation('common.actions');
    const { t: tl } = useTranslation('common.labels');
    const me = useUserContext();
    const [existingOwners, setExistingOwners] = useState<Owner[]>(selectedSource?.ownership?.owners || []);
    const initialOwners = useMemo(() => {
        if (!isEditing && me.user && me.loaded) {
            return [me.user];
        }
        return existingOwners.map((owner) => owner.owner);
    }, [existingOwners, isEditing, me.user, me.loaded]);
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>(initialOwners.map((actor) => actor.urn));
    const [areOwnersInitialized, setAreOwnersInitialized] = useState<boolean>(false);

    useEffect(() => {
        setExistingOwners(selectedSource?.ownership?.owners || []);
    }, [selectedSource?.ownership?.owners]);

    const setName = (stagedName: string) => {
        const newState: SourceBuilderState = {
            ...state,
            name: stagedName,
        };
        updateState(newState);
    };

    const setOwners = useCallback(
        (newOwners: Entity[]) => {
            const newState: SourceBuilderState = {
                ...state,
                owners: newOwners,
            };
            updateState(newState);
        },
        [updateState, state],
    );

    // Initialize owners in parent state now that ActorsSearchSelect is controlled-only.
    useEffect(() => {
        if (me.loaded && !isEditing && !areOwnersInitialized) {
            setOwners(initialOwners);
            setSelectedOwnerUrns(initialOwners.map((actor) => actor.urn));
            setAreOwnersInitialized(true);
            return;
        }
        if (isEditing && selectedSource && !areOwnersInitialized) {
            setOwners(initialOwners);
            setSelectedOwnerUrns(initialOwners.map((actor) => actor.urn));
            setAreOwnersInitialized(true);
        }
    }, [initialOwners, isEditing, me.loaded, areOwnersInitialized, selectedSource, setOwners]);

    const setExecutorId = (execId: string) => {
        const newState: SourceBuilderState = {
            ...state,
            config: {
                ...state.config,
                executorId: execId,
            },
        };
        updateState(newState);
    };

    const setVersion = (version: string) => {
        const newState: SourceBuilderState = {
            ...state,
            config: {
                ...state.config,
                version,
            },
        };
        updateState(newState);
    };

    const setDebugMode = (debugMode: boolean) => {
        const newState: SourceBuilderState = {
            ...state,
            config: {
                ...state.config,
                debugMode,
            },
        };
        updateState(newState);
    };

    const retrieveExtraEnvs = () => {
        const extraArgs: StringMapEntryInput[] = state.config?.extraArgs ? [...state.config?.extraArgs] : [];
        const index: number = extraArgs.findIndex((entry) => entry.key === ExtraEnvKey) as number;
        if (index > -1) {
            return extraArgs[index].value;
        }
        return '';
    };

    const setExtraEnvs = (envs: string) => {
        let extraArgs: StringMapEntryInput[] = state.config?.extraArgs ? [...state.config?.extraArgs] : [];
        const indxOfEnvVars: number = extraArgs.findIndex((entry) => entry.key === ExtraEnvKey) as number;
        const value = { key: ExtraEnvKey, value: envs };
        if (indxOfEnvVars > -1) {
            extraArgs[indxOfEnvVars] = value;
        } else {
            extraArgs = [...extraArgs, value];
        }
        const newState: SourceBuilderState = {
            ...state,
            config: {
                ...state.config,
                extraArgs,
            },
        };
        updateState(newState);
    };

    const retrieveExtraDataHubPlugins = () => {
        const extraArgs: StringMapEntryInput[] = state.config?.extraArgs ? [...state.config?.extraArgs] : [];
        const index: number = extraArgs.findIndex((entry) => entry.key === ExtraPluginKey) as number;
        if (index > -1) {
            return extraArgs[index].value;
        }
        return '';
    };

    const setExtraDataHubPlugins = (plugins: string) => {
        let extraArgs: StringMapEntryInput[] = state.config?.extraArgs ? [...state.config?.extraArgs] : [];
        const indxOfPlugins: number = extraArgs.findIndex((entry) => entry.key === ExtraPluginKey) as number;
        const value = { key: ExtraPluginKey, value: plugins };
        if (indxOfPlugins > -1) {
            extraArgs[indxOfPlugins] = value;
        } else {
            extraArgs = [...extraArgs, value];
        }
        const newState: SourceBuilderState = {
            ...state,
            config: {
                ...state.config,
                extraArgs,
            },
        };
        updateState(newState);
    };

    const retrieveExtraReqs = () => {
        const extraArgs: StringMapEntryInput[] = state.config?.extraArgs ? [...state.config?.extraArgs] : [];
        const index: number = extraArgs.findIndex((entry) => entry.key === ExtraReqKey) as number;
        if (index > -1) {
            return extraArgs[index].value;
        }
        return '';
    };

    const setExtraReqs = (reqs: string) => {
        let extraArgs: StringMapEntryInput[] = state.config?.extraArgs ? [...state.config?.extraArgs] : [];
        const indxOfReqs: number = extraArgs.findIndex((entry) => entry.key === ExtraReqKey) as number;
        const value = { key: ExtraReqKey, value: reqs };
        if (indxOfReqs > -1) {
            extraArgs[indxOfReqs] = value;
        } else {
            extraArgs = [...extraArgs, value];
        }
        const newState: SourceBuilderState = {
            ...state,
            config: {
                ...state.config,
                extraArgs,
            },
        };
        updateState(newState);
    };

    const onClickCreate = (shouldRun?: boolean) => {
        if (state.name !== undefined && state.name.length > 0) {
            submit(shouldRun);
            setSelectedOwnerUrns([]);
        }
    };

    const onUpdateOwners = useCallback(
        (actors: ActorEntity[]) => {
            setOwners(actors);
            setSelectedOwnerUrns(actors.map((actor) => actor.urn));
        },
        [setOwners],
    );

    const handleBlur = (event: React.FocusEvent<HTMLInputElement>, setterFunction: (value: string) => void) => {
        const trimmedValue = event.target.value.trim();
        setterFunction(trimmedValue);
    };

    return (
        <>
            <RequiredFieldForm layout="vertical">
                <Form.Item
                    label={
                        <LabelContainer>
                            <Text>{tl('name')}</Text>
                            <Text color="textError"> *</Text>
                        </LabelContainer>
                    }
                    style={{ marginBottom: 16 }}
                >
                    <Typography.Paragraph data-testid="give-source-name-heading">
                        {t('nameStep.giveName')}
                    </Typography.Paragraph>
                    <Input
                        data-testid="source-name-input"
                        className="source-name-input"
                        placeholder={t('nameStep.namePlaceholder')}
                        value={state.name}
                        onChange={(event) => setName(event.target.value)}
                        onBlur={(event) => handleBlur(event, setName)}
                    />
                </Form.Item>

                <Form.Item
                    label={
                        <LabelContainer>
                            <Text>{tl('owners')}</Text>
                        </LabelContainer>
                    }
                    style={{ marginBottom: 16 }}
                >
                    <ActorsSearchSelect selectedActorUrns={selectedOwnerUrns} onUpdate={onUpdateOwners} />
                </Form.Item>

                <Collapse ghost>
                    <Collapse.Panel
                        header={
                            <Typography.Text type="secondary" data-testid="advanced-settings-header">
                                {t('nameStep.advanced')}
                            </Typography.Text>
                        }
                        key="1"
                    >
                        {/* NOTE: Executor ID is OSS-only, used by actions pod */}
                        <Form.Item label={<Typography.Text strong>{t('nameStep.executorId.label')}</Typography.Text>}>
                            <Typography.Paragraph>{t('nameStep.executorId.description')}</Typography.Paragraph>
                            <Input
                                placeholder={EXECUTOR_ID_PLACEHOLDER}
                                value={state.config?.executorId || ''}
                                onChange={(event) => setExecutorId(event.target.value)}
                                onBlur={(event) => handleBlur(event, setExecutorId)}
                            />
                        </Form.Item>
                        <Form.Item label={<Typography.Text strong>{t('nameStep.cliVersion.label')}</Typography.Text>}>
                            <Typography.Paragraph>{t('nameStep.cliVersion.description')}</Typography.Paragraph>
                            <Input
                                data-testid="cli-version-input"
                                className="cli-version-input"
                                placeholder={t('nameStep.cliVersion.placeholder')}
                                value={state.config?.version || ''}
                                onChange={(event) => setVersion(event.target.value)}
                                onBlur={(event) => handleBlur(event, setVersion)}
                            />
                        </Form.Item>
                        <Form.Item label={<Typography.Text strong>{t('nameStep.debugMode.label')}</Typography.Text>}>
                            <Typography.Paragraph>{t('nameStep.debugMode.description')}</Typography.Paragraph>
                            <Checkbox
                                checked={state.config?.debugMode || false}
                                onChange={(event) => setDebugMode(event.target.checked)}
                            />
                        </Form.Item>
                        <Form.Item label={<Typography.Text strong>{t('nameStep.extraEnvVars.label')}</Typography.Text>}>
                            <Typography.Paragraph>{t('nameStep.extraEnvVars.description')}</Typography.Paragraph>
                            <Input
                                data-testid="extra-args-input"
                                placeholder={EXTRA_ENV_PLACEHOLDER}
                                value={retrieveExtraEnvs()}
                                onChange={(event) => setExtraEnvs(event.target.value)}
                                onBlur={(event) => handleBlur(event, setExtraEnvs)}
                            />
                        </Form.Item>
                        <Form.Item label={<Typography.Text strong>{t('nameStep.extraPlugins.label')}</Typography.Text>}>
                            <Typography.Paragraph>{t('nameStep.extraPlugins.description')}</Typography.Paragraph>
                            <Input
                                data-testid="extra-pip-plugin-input"
                                placeholder={EXTRA_ARGS_PLACEHOLDER}
                                value={retrieveExtraDataHubPlugins()}
                                onChange={(event) => setExtraDataHubPlugins(event.target.value)}
                                onBlur={(event) => handleBlur(event, setExtraDataHubPlugins)}
                            />
                        </Form.Item>
                        <Form.Item
                            label={<Typography.Text strong>{t('nameStep.extraPipLibraries.label')}</Typography.Text>}
                        >
                            <Typography.Paragraph>{t('nameStep.extraPipLibraries.description')}</Typography.Paragraph>
                            <Input
                                data-testid="extra-pip-reqs-input"
                                placeholder={EXTRA_PIP_PLACEHOLDER}
                                value={retrieveExtraReqs()}
                                onChange={(event) => setExtraReqs(event.target.value)}
                                onBlur={(event) => handleBlur(event, setExtraReqs)}
                            />
                        </Form.Item>
                    </Collapse.Panel>
                </Collapse>
            </RequiredFieldForm>
            <ControlsContainer>
                <Button variant="outline" color="gray" onClick={prev}>
                    {tc('previous')}
                </Button>
                <ModalButtonContainer>
                    <Button
                        variant="outline"
                        data-testid="ingestion-source-save-button"
                        disabled={!(state.name !== undefined && state.name.length > 0)}
                        onClick={() => onClickCreate(false)}
                    >
                        {tc('save')}
                    </Button>
                    <Tooltip showArrow={false} title={t('nameStep.saveAndRunTooltip')}>
                        <Button
                            disabled={!(state.name !== undefined && state.name.length > 0)}
                            onClick={() => onClickCreate(true)}
                            data-testid="ingestion-source-save-and-run-button"
                        >
                            {t('nameStep.saveAndRun')}
                        </Button>
                    </Tooltip>
                </ModalButtonContainer>
            </ControlsContainer>
        </>
    );
};
