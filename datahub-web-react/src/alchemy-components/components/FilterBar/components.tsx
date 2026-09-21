import React, { useCallback, useEffect, useMemo, useRef } from 'react';
import styled from 'styled-components';

import useClickOutside from '@components/components/Utils/ClickOutside/useClickOutside';
import { radius, spacing, typography, zIndices } from '@components/theme';

export const Container = styled.div`
    display: flex;
    width: 100%;
    flex-direction: column;
    gap: ${spacing.xsm};
`;

export const GroupContainer = styled.div<{ $nested: boolean }>`
    display: flex;
    flex-direction: column;
    gap: ${spacing.xsm};
    padding: ${(props) => (props.$nested ? spacing.sm : spacing.none)};
    border: ${(props) => (props.$nested ? `1px solid ${props.theme.colors.border}` : 'none')};
    border-radius: ${radius.md};
    background: ${(props) => (props.$nested ? props.theme.colors.bgSurface : 'transparent')};
`;

export const GroupHeader = styled.div`
    display: flex;
    min-height: 24px;
    align-items: center;
    justify-content: space-between;
`;

export const FiltersRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: ${spacing.xsm};
`;

export const GroupActions = styled.div`
    display: flex;
    align-items: center;
    gap: ${spacing.xxsm};
`;

export const MatchControls = styled.div`
    display: flex;
    align-items: center;
    gap: ${spacing.xxsm};
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: ${typography.fontSizes.sm};
`;

/** Shared reset for the inline text affordances: no button chrome, just a hit target. */
const inlineTrigger = `
    border: 0;
    background: transparent;
    font: inherit;
    cursor: pointer;
`;

export const MatchButton = styled.button`
    ${inlineTrigger}
    padding: 2px ${spacing.xxsm};
    border-radius: ${radius.sm};
    color: ${(props) => props.theme.colors.text};
    font-weight: ${typography.fontWeights.semiBold};

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
    }
`;

export const GhostTrigger = styled.button`
    ${inlineTrigger}
    display: inline-flex;
    min-height: 32px;
    align-items: center;
    gap: ${spacing.xxsm};
    padding: 0 ${spacing.xsm};
    border-radius: ${radius.md};
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: ${typography.fontSizes.md};

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
        color: ${(props) => props.theme.colors.text};
    }
`;

export const Chip = styled.div`
    display: inline-flex;
    min-height: 32px;
    align-items: center;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: ${radius.md};
    background: ${(props) => props.theme.colors.bgSurface};
    color: ${(props) => props.theme.colors.text};
    font-size: ${typography.fontSizes.md};
`;

export const FieldName = styled.span`
    padding: 0 ${spacing.xsm};
    font-weight: ${typography.fontWeights.semiBold};
`;

export const ChipSegment = styled.button<{ $emphasized?: boolean; $placeholder?: boolean }>`
    ${inlineTrigger}
    display: inline-flex;
    height: 30px;
    max-width: 320px;
    align-items: center;
    gap: ${spacing.xxsm};
    padding: 0 ${spacing.xsm};
    border-left: 1px solid ${(props) => props.theme.colors.border};
    color: ${(props) => (props.$placeholder ? props.theme.colors.textPlaceholder : props.theme.colors.text)};
    font-weight: ${(props) => (props.$emphasized ? typography.fontWeights.semiBold : typography.fontWeights.normal)};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
    }
`;

export const ValueIconStack = styled.span`
    display: inline-flex;
    flex-shrink: 0;
    align-items: center;
`;

export const ValueIconStackItem = styled.span`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    margin-left: -6px;
    border-radius: ${radius.full};
    background: ${(props) => props.theme.colors.bgSurface};
    box-shadow: 0 0 0 1.5px ${(props) => props.theme.colors.bgSurface};

    &:first-child {
        margin-left: 0;
    }

    > img,
    > svg {
        width: 16px;
        height: 16px;
        border-radius: ${radius.sm};
    }
`;

export const RemoveButton = styled.button`
    ${inlineTrigger}
    display: flex;
    width: 30px;
    height: 30px;
    align-items: center;
    justify-content: center;
    border-left: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 0 ${radius.md} ${radius.md} 0;
    color: ${(props) => props.theme.colors.icon};

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
        color: ${(props) => props.theme.colors.iconError};
    }
`;

export const PopoverAnchor = styled.div`
    position: relative;
    display: inline-flex;
`;

export const PopoverPanel = styled.div<{ $width: number }>`
    position: absolute;
    z-index: ${zIndices.popover};
    top: calc(100% + ${spacing.xxsm});
    left: 0;
    display: flex;
    width: ${(props) => props.$width}px;
    max-width: calc(100vw - ${spacing.xlg});
    flex-direction: column;
    gap: ${spacing.xxsm};
    padding: ${spacing.xsm};
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: ${radius.md};
    background: ${(props) => props.theme.colors.bgOverlay};
    box-shadow: ${(props) => props.theme.colors.shadowMd};
`;

export const OptionList = styled.div`
    display: flex;
    max-height: 288px;
    flex-direction: column;
    overflow-y: auto;
`;

export const OptionRow = styled.button`
    ${inlineTrigger}
    display: flex;
    width: 100%;
    min-height: 32px;
    flex-shrink: 0;
    align-items: center;
    gap: ${spacing.xsm};
    padding: ${spacing.xxsm} ${spacing.xsm};
    border-radius: ${radius.sm};
    color: ${(props) => props.theme.colors.text};
    font-size: ${typography.fontSizes.md};
    text-align: left;

    &:hover:not(:disabled) {
        background: ${(props) => props.theme.colors.bgHover};
    }

    &:disabled {
        color: ${(props) => props.theme.colors.textDisabled};
        cursor: not-allowed;
    }
`;

export const OptionContent = styled.span`
    display: flex;
    min-width: 0;
    flex: 1;
    flex-direction: column;
`;

export const OptionLabel = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

export const OptionDescription = styled.span`
    overflow: hidden;
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: ${typography.fontSizes.sm};
    text-overflow: ellipsis;
    white-space: nowrap;
`;

export const OptionCount = styled.span`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: ${typography.fontSizes.sm};
`;

export const MenuState = styled.div`
    display: flex;
    min-height: 40px;
    align-items: center;
    justify-content: center;
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: ${typography.fontSizes.sm};
`;

export const SectionLabel = styled.div`
    flex-shrink: 0;
    padding: ${spacing.xsm} ${spacing.xsm} ${spacing.xxsm};
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: ${typography.fontSizes.sm};
    font-weight: ${typography.fontWeights.semiBold};
`;

type FilterPopoverProps = {
    isOpen: boolean;
    onClose: () => void;
    trigger: React.ReactNode;
    width?: number;
};

/**
 * Anchored overlay used by every menu in the filter bar. Kept local so the bar stays
 * free of third party overlay libraries and inherits alchemy tokens directly.
 */
export function FilterPopover({
    isOpen,
    onClose,
    trigger,
    width = 280,
    children,
}: React.PropsWithChildren<FilterPopoverProps>): JSX.Element {
    const anchorRef = useRef<HTMLDivElement>(null);
    const clickOutsideOptions = useMemo(() => ({ wrappers: [anchorRef] }), []);
    const onClickOutside = useCallback(() => {
        if (isOpen) onClose();
    }, [isOpen, onClose]);

    useClickOutside(onClickOutside, clickOutsideOptions);

    useEffect(() => {
        if (!isOpen) return undefined;
        const onKeyDown = (event: KeyboardEvent) => {
            if (event.key === 'Escape') onClose();
        };
        document.addEventListener('keydown', onKeyDown);
        return () => document.removeEventListener('keydown', onKeyDown);
    }, [isOpen, onClose]);

    return (
        <PopoverAnchor ref={anchorRef}>
            {trigger}
            {isOpen && <PopoverPanel $width={width}>{children}</PopoverPanel>}
        </PopoverAnchor>
    );
}
