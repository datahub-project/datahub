import { Button, Collapse, Form, Input, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { SourceBuilderState, StepProps } from './types';

const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

const SaveButton = styled(Button)`
    margin-right: 15px;
`;

export const NameSourceStep = ({ state, updateState, prev, submit }: StepProps) => {
    const setName = (stagedName: string) => {
        const newState: SourceBuilderState = {
            ...state,
            name: stagedName,
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

    const onClickCreate = (shouldRun?: boolean) => {
        if (state.name !== undefined && state.name.length > 0) {
            submit(shouldRun);
        }
    };

    return (
        <>
            <Form layout="vertical">
                <Form.Item
                    required
                    label={
                        <Typography.Text strong style={{ marginBottom: 0 }}>
                            Name
                        </Typography.Text>
                    }
                    style={{ marginBottom: 8 }}
                >
                    <Typography.Paragraph>Give this ingestion source a name.</Typography.Paragraph>
                    <Input
                        placeholder="My Redshift Source #2"
                        value={state.name}
                        onChange={(event) => setName(event.target.value)}
                    />
                </Form.Item>
                <Collapse ghost>
                    <Collapse.Panel header={<Typography.Text type="secondary">Advanced</Typography.Text>} key="1">
                        <Form.Item label={<Typography.Text strong>Executor Id</Typography.Text>}>
                            <Typography.Paragraph>
                                Provide the executor id to route execution requests to. The built-in DataHub executor id
                                is &apos;default&apos;. Do not change this unless you have configured a custom executor.
                            </Typography.Paragraph>
                            <Input
                                placeholder="default"
                                value={state.config?.executorId || ''}
                                onChange={(event) => setExecutorId(event.target.value)}
                            />
                        </Form.Item>
                        <Form.Item label={<Typography.Text strong>CLI Version</Typography.Text>}>
                            <Typography.Paragraph>
                                Advanced: Provide a custom CLI version to use for ingestion.
                            </Typography.Paragraph>
                            <Input
                                placeholder="0.8.42"
                                value={state.config?.version || ''}
                                onChange={(event) => setVersion(event.target.value)}
                            />
                        </Form.Item>
                    </Collapse.Panel>
                </Collapse>
            </Form>
            <ControlsContainer>
                <Button onClick={prev}>Previous</Button>
                <div>
                    <SaveButton
                        disabled={!(state.name !== undefined && state.name.length > 0)}
                        onClick={() => onClickCreate(false)}
                    >
                        Save
                    </SaveButton>
                    <Button
                        disabled={!(state.name !== undefined && state.name.length > 0)}
                        onClick={() => onClickCreate(true)}
                        type="primary"
                    >
                        Save & Run
                    </Button>
                </div>
            </ControlsContainer>
        </>
    );
};
