import React from 'react';
import styled from 'styled-components';

const Wrapper = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 0;
`;

interface Props {
    children?: React.ReactNode;
}

export function CustomItemWrapper({ children }: Props) {
    // Custom items handle their own clicks; stop them reaching the menu item, which would close the
    // menu. Only propagation is stopped, so links, labels and inputs inside keep their defaults.
    const stopMenuClick = (e: React.MouseEvent) => e.stopPropagation();

    return <Wrapper onClick={stopMenuClick}>{children}</Wrapper>;
}
