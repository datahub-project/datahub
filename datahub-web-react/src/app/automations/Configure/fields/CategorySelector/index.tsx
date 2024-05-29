import React from 'react';

import { Select, Input, Typography } from 'antd';
import styled from 'styled-components';

import { TestCategory, TEST_CATEGORIES } from '../../constants';

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

export const CategorySelector = ({ categorySelected, setCategorySelected }: any) => (
	<>
		<Select
			value={categorySelected}
			onChange={(value) => setCategorySelected(value)}
		>
			{TEST_CATEGORIES.map((category) => (
				<Select.Option key={category.name} value={category.name}>
					<Typography.Text><strong>{category.name}</strong></Typography.Text>
					<OptionDescription>{category.description}</OptionDescription>
				</Select.Option>
			))}
		</Select>
		{categorySelected === TestCategory.CUSTOM && (
			<Input
				type="text"
				onChange={(e) => setCategorySelected(e.target.value)}
				placeholder="Enter a custom category…"
				style={{ marginTop: '8px' }}
			/>
		)}
	</>
);