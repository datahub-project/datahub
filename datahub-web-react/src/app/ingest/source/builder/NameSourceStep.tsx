import { Button, Collapse, Form, Input, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { SourceBuilderState, StepProps } from './types';

const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

export const NameSourceStep = ({ state, updateState, prev, submit }: StepProps) => {
    const setName = (stagedName: string) => {
        const newState: SourceBuilderState = {
            ...state,
            name: stagedName,
        };
        updateState(newState);
    };

    const setExecutorId = (executorId: string) => {
        const newState: SourceBuilderState = {
            ...state,
            config: {
                ...state.config,
                executorId,
            },
        };
        updateState(newState);
    };

    const onClickCreate = () => {
        if (state.name !== undefined && state.name.length > 0) {
            submit();
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
                                Provide the executor id to route execution requests to.
                            </Typography.Paragraph>
                            <Input
                                placeholder="default"
                                value={state.config?.executorId || ''}
                                onChange={(event) => setExecutorId(event.target.value)}
                            />
                        </Form.Item>
                    </Collapse.Panel>
                </Collapse>
            </Form>
            <ControlsContainer>
                <Button onClick={prev}>Previous</Button>
                <Button disabled={!(state.name !== undefined && state.name.length > 0)} onClick={onClickCreate}>
                    Done
                </Button>
            </ControlsContainer>
        </>
    );
};
