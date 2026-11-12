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
    ({ children, isOpen, isActive, onClear, showClear, ...props }, ref) => {
        const onClearHandler = useCallback(
            (e: React.MouseEvent) => {
                e.stopPropagation();
                onClear?.();
            },
            [onClear],
        );

        return (
            <SearchFilterLabel ref={ref} $isActive={!!isActive} $isOpen={!!isOpen} {...props}>
                {children}
                {showClear ? <ClearButton onClick={onClearHandler} /> : null}
                <StyledIcon icon={CaretDown} size="md" rotate={isOpen ? '180' : '0'} />
            </SearchFilterLabel>
        );
    },
);

SearchFilterBase.displayName = 'SearchFilterBase';
