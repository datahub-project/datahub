import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';
import { pluralize } from '../shared/textUtil';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

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

const getColor = (enabled?: boolean, active?: boolean) => {
    let color = '#b0a2c2';
    if (enabled) {
        if (active) {
            color = 'white';
        } else {
            color = '#81879F';
        }
    }
    return color;
};

export const PillContainer = styled.div<{ enabled?: boolean; active?: boolean; isHighlightedTextPresent: boolean }>`
    height: 24px;
    padding-left: 8px;
    padding-right: ${({ isHighlightedTextPresent }) => (isHighlightedTextPresent ? '0px' : '8px')};
    background-color: ${({ active }) => (active ? `${REDESIGN_COLORS.TITLE_PURPLE}` : '#f7f7f7')};
    cursor: pointer;
    border-radius: 20px;
    text-align: center;
    display: flex;
    flex-direction: row;
    justify-content: center;
    align-items: center;
    gap: 6px;
    color: ${(props) => getColor(props.enabled, props.active)};
    font-size: 10px;
    font-weight: 400;

    & svg {
        font-size: 12px;
        color: ${(props) => getColor(props.enabled, props.active)};
        fill: ${(props) => getColor(props.enabled, props.active)};
    }

    :hover {
        color: ${({ enabled }) => (enabled ? 'white' : '#b0a2c2')};
        background-color: ${({ enabled }) => (enabled ? `${REDESIGN_COLORS.TITLE_PURPLE}` : '#f7f7f7')};

        svg {
            color: ${({ enabled }) => (enabled ? 'white' : '#b0a2c2')};
            fill: ${({ enabled }) => (enabled ? 'white' : '#b0a2c2')};
        }

        >div: last-child {
            color: white;
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
    background-color: ${({ active }) => (active ? 'rgba(255, 255, 255, 0.2)' : '#eee')};
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
