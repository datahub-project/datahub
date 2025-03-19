import React from 'react';
import { Icon } from '@components';
import { borders, colors, radius, spacing, typography } from '@src/alchemy-components/theme';
import styled from 'styled-components';
import { SelectSizeOptions } from '../../types';

const SearchInputContainer = styled.div({
    position: 'relative',
    width: '100%',
    display: 'flex',
    justifyContent: 'center',
});

const SearchInput = styled.input({
    width: '100%',
    borderRadius: radius.md,
    border: `1px solid ${colors.gray[200]}`,
    color: colors.gray[500],
    fontFamily: typography.fonts.body,
    fontSize: typography.fontSizes.sm,
    padding: spacing.xsm,
    paddingRight: spacing.xlg,

    '&:focus': {
        borderColor: colors.violet[200],
        outline: `${borders['1px']} ${colors.violet[200]}`,
    },
});

const SearchIcon = styled(Icon)({
    position: 'absolute',
    right: spacing.sm,
    top: '50%',
    transform: 'translateY(-50%)',
    pointerEvents: 'none',
});

interface DropdownSearchBarProps {
    placeholder?: string;
    value?: string;
    size?: SelectSizeOptions;
    onChange?: (value: string) => void;
}

export default function DropdownSearchBar({ placeholder, value, size, onChange }: DropdownSearchBarProps) {
    return (
        <SearchInputContainer>
            <SearchInput
                type="text"
                placeholder={placeholder || 'Search...'}
                value={value}
                onChange={(e) => onChange?.(e.target.value)}
                style={{ fontSize: size || 'md' }}
            />
            <SearchIcon icon="Search" size={size} color="gray" />
        </SearchInputContainer>
    );
}
