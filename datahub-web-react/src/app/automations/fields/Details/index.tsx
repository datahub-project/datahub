import React from 'react';
import styled from 'styled-components';
import { Info } from 'phosphor-react';
import { Input, Tooltip, Collapse } from 'antd';

import type { ComponentBaseProps } from '@app/automations/types';

import { CategorySelector } from '../CategorySelector';

const { Panel } = Collapse;

const Container = styled.div`
    display: grid;
    gap: 12px;

    & span {
        padding-left: 4px;
    }
`;

const StyledLabel = styled.label`
    display: flex !important;
    align-items: center;
    gap: 4px;
`;

// State Type (ensures the state is correctly applied across templates)
export type DetailsStateType = {
    name?: string;
    description?: string;
    category?: string;
    executorId?: string;
};

// Component
export const Details = ({ state, props, passStateToParent }: ComponentBaseProps) => {
    // Defined in @app/automations/fields/index
    const { name: nameProps, description: descriptionProps, category: categoryProps, executor } = props;

    // Defined in @app/automations/fields/index
    const { name, description, category, executorId } = state as DetailsStateType;

    // Handle passing state to parent
    const handleChange = (key: string, value: string) => {
        passStateToParent({ ...state, [key]: value });
    };

    return (
        <Container>
            <div>
                <label aria-required={nameProps.isRequired} htmlFor="name">
                    {nameProps.label}
                    {nameProps.isRequired && <span style={{ color: 'red' }}>*</span>}
                </label>
                <Input
                    type="text"
                    name="name"
                    value={name}
                    placeholder={nameProps.placeholder}
                    onChange={(e) => handleChange('name', e.target.value)}
                    required={nameProps.isRequired}
                />
            </div>
            <div>
                <label aria-required={descriptionProps.isRequired} htmlFor="description">
                    {descriptionProps.label}
                    {descriptionProps.isRequired && <span style={{ color: 'red' }}>*</span>}
                </label>
                <Input.TextArea
                    name="description"
                    value={description}
                    placeholder={descriptionProps.placeholder}
                    onChange={(e) => handleChange('description', e.target.value)}
                    required={descriptionProps.isRequired}
                />
            </div>
            {!categoryProps.isHidden && (
                <div>
                    <label aria-required={categoryProps.isRequired} htmlFor="category">
                        {categoryProps.label}
                        {categoryProps.isRequired && <span style={{ color: 'red' }}>*</span>}
                    </label>
                    <CategorySelector
                        categorySelected={category}
                        setCategorySelected={(value) => handleChange('category', value)}
                    />
                </div>
            )}
            <Collapse collapsible="icon">
                <Panel header="Advanced" key="details-advanced">
                    <div>
                        <StyledLabel htmlFor="executorId">
                            {executor.label}
                            <Tooltip title={executor.tooltip}>
                                <Info />
                            </Tooltip>
                        </StyledLabel>
                        <Input
                            type="text"
                            name="executorId"
                            value={executorId}
                            placeholder={executor.placeholder}
                            onChange={(e) => handleChange('executorId', e.target.value)}
                            required={false}
                        />
                    </div>
                </Panel>
            </Collapse>
        </Container>
    );
};
