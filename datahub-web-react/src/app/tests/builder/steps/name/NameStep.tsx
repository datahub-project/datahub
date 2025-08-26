import { Form, Input, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { CategorySelect } from '@app/tests/builder/steps/name/CategorySelect';
import { StepProps } from '@app/tests/builder/types';
import { DEFAULT_TEST_CATEGORY, TestCategory } from '@app/tests/constants';
import { isCustomCategory, isSupportedCategory } from '@app/tests/utils';
import { Button } from '@src/alchemy-components';

const StyledForm = styled(Form)`
    max-width: 400px;
`;

const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

const SaveButton = styled(Button)`
    margin-right: 15px;
`;

export const NameStep = ({ state, updateState, prev, submit }: StepProps) => {
    const [showCustomCategory, setShowCustomCategory] = useState(isCustomCategory(state?.category));

    const setName = (name: string) => {
        updateState({
            ...state,
            name,
        });
    };

    const setCategory = (category: string) => {
        if (category === TestCategory.CUSTOM || !isSupportedCategory(category)) {
            setShowCustomCategory(true);
        } else {
            setShowCustomCategory(false);
        }
        updateState({
            ...state,
            category,
        });
    };

    const setDescription = (description: string) => {
        updateState({
            ...state,
            description,
        });
    };

    const onClickCreate = () => {
        if (state.category?.length && state.name?.length) {
            submit();
        }
    };

    return (
        <>
            <StyledForm layout="vertical">
                <Form.Item required label={<Typography.Text strong>Name</Typography.Text>}>
                    <Input
                        placeholder="A name for your test"
                        value={state.name}
                        onChange={(event) => setName(event.target.value)}
                    />
                </Form.Item>
                <Form.Item required label={<Typography.Text strong>Category</Typography.Text>}>
                    <CategorySelect
                        categoryName={state.category || DEFAULT_TEST_CATEGORY}
                        onSelect={(newValue) => setCategory(newValue)}
                    />
                </Form.Item>
                {showCustomCategory && (
                    <Form.Item required label={<Typography.Text strong>Custom Category</Typography.Text>}>
                        <Input
                            placeholder="The category of your test"
                            value={state.category}
                            onChange={(event) => setCategory(event.target.value)}
                        />
                    </Form.Item>
                )}
                <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                    <Input.TextArea
                        placeholder="The description for your test"
                        value={state.description || undefined}
                        onChange={(event) => setDescription(event.target.value)}
                    />
                </Form.Item>
            </StyledForm>
            <ControlsContainer>
                <Button variant="outline" color="gray" onClick={prev}>
                    Back
                </Button>
                <SaveButton
                    disabled={!(state.name !== undefined && state.name.length > 0)}
                    onClick={() => onClickCreate()}
                >
                    Save
                </SaveButton>
            </ControlsContainer>
        </>
    );
};
