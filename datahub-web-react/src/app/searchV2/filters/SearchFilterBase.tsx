import { Icon } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/ssr/CaretDown';
import React, { HTMLAttributes, useCallback } from 'react';
import styled from 'styled-components';

import { ClearButton } from '@components/components/Select/components';

import { SearchFilterLabel } from '@app/searchV2/filters/styledComponents';

const StyledIcon = styled(Icon)`
    color: ${(props) => props.theme.colors.text};
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

        // The pill can't be a <button>: it holds the clear button. Enter and Space click it instead,
        // which opens the menu or popover it triggers.
        const onKeyDownHandler = useCallback(
            (e: React.KeyboardEvent<HTMLDivElement>) => {
                onKeyDown?.(e);
                if (e.defaultPrevented || e.target !== e.currentTarget) return;
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    e.currentTarget.click();
                }
            },
            [onKeyDown],
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
                {showClear ? <ClearButton onClick={onClearHandler} /> : null}
                <StyledIcon icon={CaretDown} size="md" rotate={isOpen ? '180' : '0'} />
            </SearchFilterLabel>
        );
    },
);

SearchFilterBase.displayName = 'SearchFilterBase';
