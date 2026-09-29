import { Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { DefaultTheme } from 'styled-components';

export const PILL_TERM_COUNT = 'common.counts:termCount';
export const PILL_TAG_COUNT = 'common.counts:tagCount';
export const PILL_OWNER_COUNT = 'common.counts:ownerCount';
export const PILL_MATCH_COUNT = 'common.counts:matchCount';
export const PILL_UPSTREAM_COLUMN_COUNT = 'pill.upstreamColumnCount';
export const PILL_DOWNSTREAM_COLUMN_COUNT = 'pill.downstreamColumnCount';

type PillCountKey =
    | typeof PILL_TERM_COUNT
    | typeof PILL_TAG_COUNT
    | typeof PILL_OWNER_COUNT
    | typeof PILL_MATCH_COUNT
    | typeof PILL_UPSTREAM_COLUMN_COUNT
    | typeof PILL_DOWNSTREAM_COLUMN_COUNT;

type Props = {
    icon: any;
    count: number;
    label: string;
    onClick?: (e: React.MouseEvent) => void;
    enabled?: boolean;
    countLabelKey: PillCountKey;
    active?: boolean;
    highlightedText?: string;
};

const computeColor = (theme: DefaultTheme, enabled?: boolean, active?: boolean) => {
    let color = theme.colors.textDisabled;
    if (enabled) {
        if (active) {
            color = theme.colors.textOnFillDefault;
        } else {
            color = theme.colors.textTertiary;
        }
    }
    return color;
};

const PillContainer = styled.div<{ enabled?: boolean; active?: boolean; isHighlightedTextPresent: boolean }>`
    height: 24px;
    padding-left: 8px;
    padding-right: ${({ isHighlightedTextPresent }) => (isHighlightedTextPresent ? '0px' : '8px')};
    background-color: ${({ active, theme }) => (active ? `${theme.colors.buttonFillBrand}` : theme.colors.bgSurface)};
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
        color: ${({ enabled, theme }) => (enabled ? theme.colors.iconOnFillBrand : theme.colors.iconDisabled)};
        background-color: ${({ enabled, theme }) =>
            enabled ? `${theme.colors.buttonFillBrand}` : theme.colors.bgSurface};

        svg {
            color: ${({ enabled, theme }) => (enabled ? theme.colors.iconOnFillBrand : theme.colors.iconDisabled)};
            fill: ${({ enabled, theme }) => (enabled ? theme.colors.iconOnFillBrand : theme.colors.iconDisabled)};
        }

        >div: last-child {
            color: ${({ theme }) => theme.colors.textOnFillBrand};
            background-color: ${({ theme }) => theme.colors.overlayOnBrand};
        }
    }
`;

const Container = styled.div`
    display: flex;
    align-items: center;
    gap: 5px;
`;

const CountContainer = styled.div<{ active?: boolean }>`
    background-color: ${({ active, theme }) => (active ? theme.colors.overlayOnBrand : theme.colors.bgSurface)};
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

const SearchPill = ({ icon, onClick, enabled, label, count, countLabelKey, active, highlightedText }: Props) => {
    const { t } = useTranslation('entity.preview');
    const isHighlightedTextPresent = !!highlightedText;
    return (
        <Tooltip title={t(countLabelKey, { count })} showArrow={false}>
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
