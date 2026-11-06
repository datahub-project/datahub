import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Text } from '@components/components/Text/Text';
import { TextProps } from '@components/components/Text/types';

const Clamp = styled.span<{ $lineClamp?: number }>`
    ${(props) =>
        props.$lineClamp &&
        `
        display: -webkit-box;
        -webkit-box-orient: vertical;
        -webkit-line-clamp: ${props.$lineClamp};
        overflow: hidden;
        overflow-wrap: anywhere;
    `}
`;

const ToggleButton = styled.button`
    padding: 0;
    border: none;
    background: none;
    cursor: pointer;
    font: inherit;
    color: ${(props) => props.theme.colors.hyperlinks};

    &:hover {
        color: ${(props) => props.theme.colors.textBrand};
    }
`;

export type ClampedTextExpandable = {
    /** Defaults to the translated "Read More". */
    expandLabel?: React.ReactNode;
    /** Defaults to the translated "Read Less". */
    collapseLabel?: React.ReactNode;
    /** Set to false to hide the toggle once expanded. Defaults to true. */
    collapsible?: boolean;
    onExpand?: () => void;
    onCollapse?: () => void;
};

export type ClampedTextProps = TextProps & {
    /** Number of lines to show before truncating with an ellipsis. */
    lineClamp: number;
    /** Show an expand/collapse toggle when (and only when) the content overflows. */
    expandable?: boolean | ClampedTextExpandable;
};

export function ClampedText({ lineClamp, expandable, children, ...textProps }: ClampedTextProps) {
    const { t } = useTranslation('common.actions');
    const clampRef = useRef<HTMLSpanElement>(null);
    const [isExpanded, setIsExpanded] = useState(false);
    const [isOverflowing, setIsOverflowing] = useState(false);

    const measure = useCallback(() => {
        const el = clampRef.current;
        // Only measurable while clamped; once expanded, keep the last result so the collapse toggle stays.
        if (el && !isExpanded) setIsOverflowing(el.scrollHeight > el.clientHeight);
    }, [isExpanded]);

    useEffect(() => measure(), [measure, children, lineClamp]);

    useEffect(() => {
        const el = clampRef.current;
        if (!el || typeof ResizeObserver === 'undefined') return undefined;
        const observer = new ResizeObserver(measure);
        observer.observe(el);
        return () => observer.disconnect();
    }, [measure]);

    const options = typeof expandable === 'object' ? expandable : {};
    const showToggle = !!expandable && isOverflowing && (!isExpanded || options.collapsible !== false);

    const onToggle = () => {
        if (isExpanded) options.onCollapse?.();
        else options.onExpand?.();
        setIsExpanded(!isExpanded);
    };

    return (
        <Text {...textProps}>
            <Clamp ref={clampRef} $lineClamp={isExpanded ? undefined : lineClamp}>
                {children}
            </Clamp>
            {showToggle && (
                <ToggleButton type="button" onClick={onToggle} aria-expanded={isExpanded}>
                    {isExpanded ? (options.collapseLabel ?? t('readLess')) : (options.expandLabel ?? t('readMore'))}
                </ToggleButton>
            )}
        </Text>
    );
}
