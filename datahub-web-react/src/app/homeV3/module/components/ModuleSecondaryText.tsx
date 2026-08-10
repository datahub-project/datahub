import React from 'react';
import styled from 'styled-components';

const Text = styled.span<{ $ellipsis?: boolean }>`
    font-size: 12px;
    line-height: 16px;
    color: ${(props) => props.theme.colors.textSecondary};
    white-space: nowrap;

    ${(props) =>
        props.$ellipsis &&
        `
            display: block;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 100%;
        `}
`;

type Props = {
    children: React.ReactNode;
    className?: string;
    ellipsis?: boolean;
};

export default function ModuleSecondaryText({ children, className, ellipsis }: Props) {
    return (
        <Text className={className} $ellipsis={ellipsis}>
            {children}
        </Text>
    );
}
