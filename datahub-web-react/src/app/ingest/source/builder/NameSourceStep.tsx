import { Button, Checkbox, Collapse, Form, Input, Typography } from 'antd';
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
                            名称
                        </Typography.Text>
                    }
                    style={{ marginBottom: 8 }}
                >
                    <Typography.Paragraph>为数据源设定名称.</Typography.Paragraph>
                    <Input
                        data-testid="source-name-input"
                        className="source-name-input"
                        placeholder="My Redshift Source #2"
                        value={state.name}
                        onChange={(event) => setName(event.target.value)}
                    />
                </Form.Item>
                <Collapse ghost>
                    <Collapse.Panel header={<Typography.Text type="secondary">高级选项</Typography.Text>} key="1">
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
                        <Form.Item label={<Typography.Text strong>CLI版本号</Typography.Text>}>
                            <Typography.Paragraph>
                                提示: 提供 CLI 版本号用于数据集成.
                            </Typography.Paragraph>
                            <Input
                                data-testid="cli-version-input"
                                className="cli-version-input"
                                placeholder="(e.g. 0.10.5)"
                                value={state.config?.version || ''}
                                onChange={(event) => setVersion(event.target.value)}
                            />
                        </Form.Item>
                        <Form.Item label={<Typography.Text strong>Debug 模式</Typography.Text>}>
                            <Typography.Paragraph>
                                提示: 开启Debug模式，获取更详细的日志信息.
                            </Typography.Paragraph>
                            <Checkbox
                                checked={state.config?.debugMode || false}
                                onChange={(event) => setDebugMode(event.target.checked)}
                            />
                        </Form.Item>
                    </Collapse.Panel>
                </Collapse>
            </Form>
            <ControlsContainer>
                <Button onClick={prev}>上一步</Button>
                <div>
                    <SaveButton
                        disabled={!(state.name !== undefined && state.name.length > 0)}
                        onClick={() => onClickCreate(false)}
                    >
                        保存
                    </SaveButton>
                    <Button
                        disabled={!(state.name !== undefined && state.name.length > 0)}
                        onClick={() => onClickCreate(true)}
                        type="primary"
                    >
                        保存并执行
                    </Button>
                </div>
            </ControlsContainer>
        </>
    );
};
