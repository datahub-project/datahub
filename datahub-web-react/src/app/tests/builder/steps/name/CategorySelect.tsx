import React from 'react';
import styled from 'styled-components';
import { Select, Typography } from 'antd';
import { isSupportedCategory } from '../../../utils';
import { TestCategory, TEST_CATEGORIES } from '../../../constants';

const OptionDescription = styled(Typography.Paragraph)`
    && {
        padding: 0px;
        margin: 0px;
        overflow-wrap: break-word;
        white-space: normal;
    }
`;

type Props = {
    categoryName: string;
    onSelect: (newValue: string) => void;
};

export const CategorySelect = ({ categoryName, onSelect }: Props) => {
    // If we know about the category, then we select it. Otherwise it's something non-default (custom) so we select CUSTOM.
    const selectedCategoryName = isSupportedCategory(categoryName) ? categoryName : TestCategory.CUSTOM;

    return (
        <Select value={selectedCategoryName} onChange={onSelect}>
            {TEST_CATEGORIES.map((category) => (
                <Select.Option key={category.name} value={category.name}>
                    <Typography.Text>{category.name}</Typography.Text>
                    <OptionDescription type="secondary">{category.description}</OptionDescription>
                </Select.Option>
            ))}
        </Select>
    );
};
