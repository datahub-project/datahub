import { radius } from '@components';
import React from 'react';
import styled from 'styled-components';

const Chrome = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    background: ${(props) => props.theme.colors.bgSurface};
    height: 28px;
    width: 28px;
    border-radius: ${radius.full};
    color: ${(props) => props.theme.colors.icon};
`;

type Props = {
    children: React.ReactNode;
    className?: string;
};

export default function ModuleIconChrome({ children, className }: Props) {
    return <Chrome className={className}>{children}</Chrome>;
}
