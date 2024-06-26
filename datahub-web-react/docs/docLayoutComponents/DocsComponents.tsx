/* 
	Docs Only Components that helps to display information in info guides.
*/

import React from 'react';

import styled from 'styled-components';

import { copyToClipboard } from './utils';
import { CopyOutlined } from '@ant-design/icons';
import { Button } from '../../src/components';

import { tokens } from '../../src/components/theme';
const { colors } = tokens;

export const Grid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 16px;
`;

export const FlexGrid = styled.div`
  display: flex;
  gap: 16px;
`;

export const Seperator = styled.div`
  height: 16px;
`;

export const Card = styled.div`
  background: ${colors.background.lightGray};
  border-radius: 8px;
  overflow: hidden;
  padding: 16px;

  a {
	display: block;
    text-decoration: none;
	color: #000;

	margin-bottom: 8px;
    font-size: 16px;
	font-weight: bold;

	&:hover {
		color: #0070f3;
		cursor: pointer;
	}
  }

  img {
    max-width: 100%;
    height: auto;
    border-bottom: 1px solid #ddd;
  }

  p {
    font-size: 14px;
	line-height: 1.5;
    color: #666;
	margin: 0;
  }
`;

export const ColorCard = styled.div<{ color: string, size?: string }>`
	display: flex;
	gap: 16px;
	align-items: center;

	${({ size }) => size === 'sm' && `
		gap: 8px;
	`}

	& span {
		display: block;
		line-height: 1.3;
	}

	& .colorChip {
		background: ${({ color }) => color};
		width: 3rem;
		height: 3rem;

		${({ size }) => size === 'sm' && `
			width: 2rem;
			height: 2rem;
			border-radius: 4px;
		`}	

		border-radius: 8px;
		box-shadow: rgba(0, 0, 0, 0.06) 0px 2px 4px 0px inset;
	}

	& .colorValue {
		display: flex;
		align-items: center;
		gap: 0;
		font-weight: bold;
		font-size: 14px;
	}

	& .hex {
		font-size: 11px;
		opacity: 0.5;
		text-transform: uppercase;
	}
`;

export const ShadowCard = styled.div<{ shadow: string }>`
		background: ${colors.background.lightGray};
		width: 100%;
		padding: 8px 4px;
		border-radius: 8px;
		display: flex;
		align-items: center;
		justify-content: center;

		& span {
			display: block;
			background: white;
			width: 2rem;
			height: 2rem;
			border-radius: 4px;
			box-shadow: ${({ shadow }) => shadow};
		}
`;

export const FontDisplayBlock = styled.div<{ font: any }>`
	p {
		font-size: ${({ font }) => font.size} !important;
		font-weight: ${({ font }) => font.weight} !important;
		font-family: ${({ font }) => font.family} !important;
		color: ${({ font }) => font.color} !important;
		margin: 0 0 8px 0;
	}
`;

export const CopyButton = (props) => (
	<div style={{ display: 'inline-block' }}>
		<Button
			variant="text"
			color="whiteAlpha"
			size="sm"
			onClick={() => copyToClipboard(props.text)}
		>
			<CopyOutlined alt="Copy to Clipboard" style={{ fontSize: '13px' }} />
		</Button>
	</div>
);