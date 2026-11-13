import { Icon } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/ssr/CaretDown';
import { X } from '@phosphor-icons/react/dist/ssr/X';
import React, { HTMLAttributes, useCallback } from 'react';
import styled from 'styled-components';

import { SearchFilterLabel } from '@app/searchV2/filters/styledComponents';

const StyledIcon = styled(Icon)`
    color: ${(props) => props.theme.colors.text};
`;

// A plain icon rather than a button: the pill is already the control, and a button inside it is a
// nested interactive control that screen readers may not announce. Keyboard users clear with Delete.
const ClearIcon = styled.span`
    display: inline-flex;
    cursor: pointer;
    color: ${(props) => props.theme.colors.text};

    &:hover {
        color: ${(props) => props.theme.colors.textSecondary};
    }
`;

interface Props extends HTMLAttributes<HTMLDivElement> {
    isOpen?: boolean;
    isActive?: boolean;
    onClear?: () => void;
    showClear?: boolean;
}

// Forwards its ref so overlays (e.g. the filter value Popover) anchor directly on the pill.
export const SearchFilterBase = React.forwardRef<HTMLDivElement, React.PropsWithChildren<Props>>(
    ({ children, isOpen, isActive, onClear, showClear, onKeyDown, ...props }, ref) => {
        const onClearHandler = useCallback(
            (e: React.MouseEvent) => {
                e.stopPropagation();
                onClear?.();
            },
            [onClear],
        );

        // The pill can't be a <button>: it holds the clear icon. Enter and Space click it instead,
        // which opens the menu or popover it triggers, and Delete or Backspace clears it.
        const onKeyDownHandler = useCallback(
            (e: React.KeyboardEvent<HTMLDivElement>) => {
                onKeyDown?.(e);
                if (e.defaultPrevented || e.target !== e.currentTarget) return;
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    e.currentTarget.click();
                } else if (showClear && onClear && (e.key === 'Delete' || e.key === 'Backspace')) {
                    e.preventDefault();
                    onClear();
                }
            },
            [onKeyDown, showClear, onClear],
        );

        return (
            <SearchFilterLabel
                ref={ref}
                role="button"
                tabIndex={0}
                aria-expanded={isOpen === undefined ? undefined : isOpen}
                $isActive={!!isActive}
                $isOpen={!!isOpen}
                onKeyDown={onKeyDownHandler}
                {...props}
            >
                {children}
                {showClear ? (
                    <ClearIcon onClick={onClearHandler} aria-hidden data-testid="search-filter-clear">
                        <Icon icon={X} size="md" />
                    </ClearIcon>
                ) : null}
                <StyledIcon icon={CaretDown} size="md" rotate={isOpen ? '180' : '0'} />
            </SearchFilterLabel>
        );
    },
);

SearchFilterBase.displayName = 'SearchFilterBase';
