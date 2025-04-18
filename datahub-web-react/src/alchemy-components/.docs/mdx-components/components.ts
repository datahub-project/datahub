/* 
	Docs Only Components that helps to display information in info guides.
*/

import styled from 'styled-components';

import theme from '@components/theme';

export const Grid = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
`;

export const FlexGrid = styled.div`
    display: flex;
    gap: 16px;
`;

export const VerticalFlexGrid = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

export const Seperator = styled.div`
    height: 16px;
`;

export const ColorCard = styled.div<{ color: string; size?: string }>`
    display: flex;
    gap: 16px;
    align-items: center;

    ${({ size }) =>
        size === 'sm' &&
        `
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

        ${({ size }) =>
            size === 'sm' &&
            `
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

export const IconGrid = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(100px, 1fr));
    gap: 16px;
    margin-top: 20px;
`;

export const IconGridItem = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: space-between;

    border: 1px solid ${theme.semanticTokens.colors['border-color']};
    border-radius: 8px;
    overflow: hidden;

    & span {
        width: 100%;
        border-top: 1px solid ${theme.semanticTokens.colors['border-color']};
        background-color: ${theme.semanticTokens.colors['subtle-bg']};
        text-align: center;
        padding: 4px 8px;
        font-size: 10px;
    }
`;

export const IconDisplayBlock = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 50px;
`;
