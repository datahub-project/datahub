import { Input, Select, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { AUTOMATION_CATEGORIES, AutomationCategory, DEFAULT_AUTOMATION_CATEGORY } from '@app/automations/constants';
import { isSupportedCategory } from '@app/automations/utils';

const OptionDescription = styled(Typography.Paragraph)`
    && {
        font-family: Mulish;
        padding: 0px;
        margin: 0px;
        overflow-wrap: break-word;
        white-space: normal;
        line-height: normal;
        max-width: 90%;
        font-weight: normal !important;
    }
`;

export const CategorySelector = ({ categorySelected, setCategorySelected, isDisabled }: any) => {
    // If we know about the category, then we select it.
    // Otherwise it's something non-default (custom) so we select CUSTOM.
    let selectedCategoryName = '';
    if (!selectedCategoryName) selectedCategoryName = DEFAULT_AUTOMATION_CATEGORY;
    if (categorySelected)
        selectedCategoryName = isSupportedCategory(categorySelected) ? categorySelected : AutomationCategory.CUSTOM;

    return (
        <>
            <Select
                value={selectedCategoryName || DEFAULT_AUTOMATION_CATEGORY}
                onChange={(value) => setCategorySelected(value)}
                disabled={isDisabled}
            >
                {AUTOMATION_CATEGORIES.map((category) => (
                    <Select.Option key={category.name} value={category.name}>
                        <Typography.Text>
                            <strong>{category.name}</strong>
                        </Typography.Text>
                        <OptionDescription>{category.description}</OptionDescription>
                    </Select.Option>
                ))}
            </Select>
            {selectedCategoryName === AutomationCategory.CUSTOM && (
                <Input
                    type="text"
                    value={categorySelected}
                    onChange={(e) => setCategorySelected(e.target.value)}
                    placeholder="Enter a custom category…"
                    style={{ marginTop: '8px' }}
                />
            )}
        </>
    );
};
