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

export function SearchFilterBase({
    children,
    isOpen,
    isActive,
    onClear,
    showClear,
    ...props
}: React.PropsWithChildren<Props>) {
    const onClearHandler = useCallback(
        (e: React.MouseEvent) => {
            e.stopPropagation();
            onClear?.();
        },
        [onClear],
    );

    return (
        <SearchFilterLabel $isActive={!!isActive} $isOpen={!!isOpen} {...props}>
            {children}
            {showClear ? <ClearButton onClick={onClearHandler} /> : null}
            <StyledIcon icon={CaretDown} size="md" rotate={isOpen ? '180' : '0'} />
        </SearchFilterLabel>
    );
}
