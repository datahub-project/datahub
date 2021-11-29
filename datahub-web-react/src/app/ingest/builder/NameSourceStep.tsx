import { Button, Input, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { BaseBuilderState, StepProps } from './types';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const SelectTemplateHeader = styled(Typography.Title)`
    && {
        margin-bottom: 16px;
    }
`;

export const NameSourceStep = ({ state, updateState, prev, submit }: StepProps) => {
    const setStagedName = (stagedName: string) => {
        const newState: BaseBuilderState = {
            ...state,
            name: stagedName,
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
            <Section>
                <SelectTemplateHeader level={5}>Name your Source</SelectTemplateHeader>
                <Typography.Text>Give this ingestion source a name.</Typography.Text>
                <Input
                    placeholder="A name for your ingestion source"
                    value={state.name}
                    onChange={(event) => setStagedName(event.target.value)}
                />
            </Section>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 8 }}>
                <Button onClick={prev}>Previous</Button>
                <Button disabled={!(state.name !== undefined && state.name.length > 0)} onClick={onClickCreate}>
                    Create
                </Button>
            </div>
        </>
    );
};
