import { Button, Text, Tooltip } from '@components';
import { Checkbox, Collapse, Form, Input, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import RemoteExecutorPoolSelector from '@app/ingest/source/builder/RemoteExecutorPoolSelector.saas';
import { useExecutorPoolSelection } from '@app/ingest/source/builder/useExecutorPoolSelection';
import { SourceBuilderState, StepProps, StringMapEntryInput } from '@app/ingestV2/source/builder/types';
import { RequiredFieldForm } from '@app/shared/form/RequiredFieldForm';
import OwnersSection, { PendingOwner } from '@app/sharedV2/owners/OwnersSection';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';

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

export const NameSourceStep = ({
    state,
    updateState,
    prev,
    submit,
    sourceRefetch,
    isEditing,
    selectedSource,
}: StepProps) => {
    const [searchPoolQuery, setSearchPoolQuery] = useState('');
    const [existingOwners, setExistingOwners] = useState<any[]>(selectedSource?.ownership?.owners || []);
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const canEditSource = !selectedSource || selectedSource.privileges?.canEdit;
    const canExecuteSource = !selectedSource || selectedSource.privileges?.canExecute;

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

    const setOwners = (newOwners: PendingOwner[]) => {
        const newState: SourceBuilderState = {
            ...state,
            owners: newOwners,
        };
        updateState(newState);
    };

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
        const extraArgs: StringMapEntryInput[] = state.config?.extraArgs ? state.config?.extraArgs : [];
        const index: number = extraArgs.findIndex((entry) => entry.key === ExtraEnvKey) as number;
        if (index > -1) {
            return extraArgs[index].value;
        }
        return '';
    };

    const setExtraEnvs = (envs: string) => {
        let extraArgs: StringMapEntryInput[] = state.config?.extraArgs ? state.config?.extraArgs : [];
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
        const extraArgs: StringMapEntryInput[] = state.config?.extraArgs ? state.config?.extraArgs : [];
        const index: number = extraArgs.findIndex((entry) => entry.key === ExtraPluginKey) as number;
        if (index > -1) {
            return extraArgs[index].value;
        }
        return '';
    };

    const setExtraDataHubPlugins = (plugins: string) => {
        let extraArgs: StringMapEntryInput[] = state.config?.extraArgs ? state.config?.extraArgs : [];
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
        const extraArgs: StringMapEntryInput[] = state.config?.extraArgs ? state.config?.extraArgs : [];
        const index: number = extraArgs.findIndex((entry) => entry.key === ExtraReqKey) as number;
        if (index > -1) {
            return extraArgs[index].value;
        }
        return '';
    };

    const setExtraReqs = (reqs: string) => {
        let extraArgs: StringMapEntryInput[] = state.config?.extraArgs ? state.config?.extraArgs : [];
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

    const handleBlur = (event: React.FocusEvent<HTMLInputElement>, setterFunction: (value: string) => void) => {
        const trimmedValue = event.target.value.trim();
        setterFunction(trimmedValue);
    };

    const { pools, loading, total } = useExecutorPoolSelection({
        searchQuery: searchPoolQuery,
        currentExecutorId: state?.config?.executorId || '',
        isEditing,
        onSetExecutorId: setExecutorId,
    });

    return (
        <>
            <RequiredFieldForm layout="vertical">
                <Form.Item
                    label={
                        <LabelContainer>
                            <Text>Name</Text>
                            <Text color="red"> *</Text>
                        </LabelContainer>
                    }
                    style={{ marginBottom: 16 }}
                >
                    <Typography.Paragraph>Give this data source a name</Typography.Paragraph>
                    <Input
                        data-testid="source-name-input"
                        className="source-name-input"
                        placeholder="My Redshift Source #2"
                        value={state.name}
                        onChange={(event) => setName(event.target.value)}
                        onBlur={(event) => handleBlur(event, setName)}
                    />
                </Form.Item>
                <OwnersSection
                    selectedOwnerUrns={selectedOwnerUrns}
                    setSelectedOwnerUrns={setSelectedOwnerUrns}
                    existingOwners={existingOwners}
                    onChange={setOwners}
                    sourceRefetch={sourceRefetch}
                    isEditForm={isEditing}
                    canEdit={!!canEditSource}
                />

                <Collapse ghost>
                    <Collapse.Panel header={<Typography.Text type="secondary">Advanced</Typography.Text>} key="1">
                        {/* NOTE: Executor ID is OSS-only, used by actions pod */}
                        <Form.Item label={<Typography.Text strong>Executor Pool</Typography.Text>}>
                            <Typography.Paragraph>
                                Choose an Executor Pool to execute this ingestion recipe.
                            </Typography.Paragraph>
                            <RemoteExecutorPoolSelector
                                value={state.config?.executorId || (isEditing ? '' : undefined)}
                                onChange={(newPoolId) => setExecutorId(newPoolId)}
                                onBlur={(newPoolId) => setExecutorId(newPoolId)}
                                pools={pools || []}
                                total={total}
                                loading={loading}
                                handleSearch={setSearchPoolQuery}
                            />
                        </Form.Item>
                        <Form.Item label={<Typography.Text strong>CLI Version</Typography.Text>}>
                            <Typography.Paragraph>
                                Advanced: Provide a custom CLI version to use for ingestion.
                            </Typography.Paragraph>
                            <Input
                                data-testid="cli-version-input"
                                className="cli-version-input"
                                placeholder="(e.g. 0.15.0)"
                                value={state.config?.version || ''}
                                onChange={(event) => setVersion(event.target.value)}
                                onBlur={(event) => handleBlur(event, setVersion)}
                            />
                        </Form.Item>
                        <Form.Item label={<Typography.Text strong>Debug Mode</Typography.Text>}>
                            <Typography.Paragraph>
                                Advanced: Turn on debug mode in order to get more verbose logs.
                            </Typography.Paragraph>
                            <Checkbox
                                checked={state.config?.debugMode || false}
                                onChange={(event) => setDebugMode(event.target.checked)}
                            />
                        </Form.Item>
                        <Form.Item label={<Typography.Text strong>Extra Enviroment Variables</Typography.Text>}>
                            <Typography.Paragraph>
                                Advanced: Set extra environment variables to an ingestion execution
                            </Typography.Paragraph>
                            <Input
                                data-testid="extra-args-input"
                                placeholder='{"MY_CUSTOM_ENV": "my_custom_value2"}'
                                value={retrieveExtraEnvs()}
                                onChange={(event) => setExtraEnvs(event.target.value)}
                                onBlur={(event) => handleBlur(event, setExtraEnvs)}
                            />
                        </Form.Item>
                        <Form.Item label={<Typography.Text strong>Extra DataHub plugins</Typography.Text>}>
                            <Typography.Paragraph>
                                Advanced: Set extra DataHub plugins for an ingestion execution
                            </Typography.Paragraph>
                            <Input
                                data-testid="extra-pip-plugin-input"
                                placeholder='["debug"]'
                                value={retrieveExtraDataHubPlugins()}
                                onChange={(event) => setExtraDataHubPlugins(event.target.value)}
                                onBlur={(event) => handleBlur(event, setExtraDataHubPlugins)}
                            />
                        </Form.Item>
                        <Form.Item label={<Typography.Text strong>Extra Pip Libraries</Typography.Text>}>
                            <Typography.Paragraph>
                                Advanced: Add extra pip libraries for an ingestion execution
                            </Typography.Paragraph>
                            <Input
                                data-testid="extra-pip-reqs-input"
                                placeholder='["sqlparse==0.4.3"]'
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
                    Previous
                </Button>
                <ModalButtonContainer>
                    <Button
                        variant="outline"
                        data-testid="ingestion-source-save-button"
                        disabled={!canEditSource || !(state.name !== undefined && state.name.length > 0)}
                        onClick={() => onClickCreate(false)}
                    >
                        Save
                    </Button>
                    <Tooltip showArrow={false} title="Save and start syncing data source">
                        <Button
                            disabled={
                                !canEditSource ||
                                !canExecuteSource ||
                                !(state.name !== undefined && state.name.length > 0)
                            }
                            onClick={() => onClickCreate(true)}
                        >
                            Save & Run
                        </Button>
                    </Tooltip>
                </ModalButtonContainer>
            </ControlsContainer>
        </>
    );
};
