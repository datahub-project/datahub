import { Tooltip } from '@components';
import React from 'react';
import styled, { DefaultTheme } from 'styled-components';

import { pluralize } from '@app/shared/textUtil';

type Props = {
    icon: any;
    count: number;
    label: string;
    onClick?: (e: React.MouseEvent) => void;
    enabled?: boolean;
    countLabel: string;
    active?: boolean;
    highlightedText?: string;
};

const computeColor = (theme: DefaultTheme, enabled?: boolean, active?: boolean) => {
    let color = theme.colors.textDisabled;
    if (enabled) {
        if (active) {
            color = theme.colors.bg;
        } else {
            color = theme.colors.textTertiary;
        }
    }
    return color;
};

export const PillContainer = styled.div<{ enabled?: boolean; active?: boolean; isHighlightedTextPresent: boolean }>`
    height: 24px;
    padding-left: 8px;
    padding-right: ${({ isHighlightedTextPresent }) => (isHighlightedTextPresent ? '0px' : '8px')};
    background-color: ${({ active, theme }) => (active ? `${theme.styles['primary-color']}` : theme.colors.bgSurface)};
    cursor: pointer;
    border-radius: 20px;
    text-align: center;
    display: flex;
    flex-direction: row;
    justify-content: center;
    align-items: center;
    gap: 6px;
    color: ${(props) => computeColor(props.theme, props.enabled, props.active)};
    font-size: 10px;
    font-weight: 400;

    & svg {
        font-size: 12px;
        color: ${(props) => computeColor(props.theme, props.enabled, props.active)};
        fill: ${(props) => computeColor(props.theme, props.enabled, props.active)};
    }

    :hover {
        color: ${({ enabled, theme }) => (enabled ? theme.colors.bg : theme.colors.textDisabled)};
        background-color: ${({ enabled, theme }) =>
            enabled ? `${theme.styles['primary-color']}` : theme.colors.bgSurface};

        svg {
            color: ${({ enabled, theme }) => (enabled ? theme.colors.bg : theme.colors.textDisabled)};
            fill: ${({ enabled, theme }) => (enabled ? theme.colors.bg : theme.colors.textDisabled)};
        }

        >div: last-child {
            color: ${({ theme }) => theme.colors.textOnFillBrand};
            background-color: rgba(255, 255, 255, 0.2);
        }
    }
`;

const Container = styled.div`
    display: flex;
    align-items: center;
    gap: 5px;
`;

const CountContainer = styled.div<{ active?: boolean }>`
    background-color: ${({ active, theme }) => (active ? 'rgba(255, 255, 255, 0.2)' : theme.colors.border)};
    border-radius: 20px;
    height: 24px;
    min-width: 35px;
    padding: 4px 8px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

const HighlightedText = styled.div`
    max-width: 100px;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
`;

// pluralize

const SearchPill = ({ icon, onClick, enabled, label, count, countLabel, active, highlightedText }: Props) => {
    const isHighlightedTextPresent = !!highlightedText;
    return (
        <Tooltip
            title={`${count} ${pluralize(count, countLabel, countLabel === 'match' ? 'es' : 's')}`}
            showArrow={false}
        >
            <PillContainer
                active={active}
                enabled={enabled}
                onClick={onClick}
                isHighlightedTextPresent={isHighlightedTextPresent}
            >
                {isHighlightedTextPresent ? (
                    <>
                        <Container>
                            {icon} {label}
                            <HighlightedText>{highlightedText}</HighlightedText>
                        </Container>
                        <CountContainer active={active}>{count}</CountContainer>
                    </>
                ) : (
                    <>
                        {icon} {label} {count}
                    </>
                )}
            </PillContainer>
        </Tooltip>
    );
};

export default SearchPill;
