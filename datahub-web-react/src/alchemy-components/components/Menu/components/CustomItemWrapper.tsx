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
    // Disable mouse events to prevent default menu behaviour
    // as custom items could have their own behaviour
    const disableMouseEvents = (e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();
    };

    return (
        <Wrapper onMouseLeave={disableMouseEvents} onMouseEnter={disableMouseEvents} onClick={disableMouseEvents}>
            {children}
        </Wrapper>
    );
}
