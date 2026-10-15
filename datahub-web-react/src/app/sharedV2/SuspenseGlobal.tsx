import React from 'react';
import styled from 'styled-components';

const FullPageBackground = styled.div`
    position: fixed;
    inset: 0;
    background: ${(props) => props.theme.colors.bgSurfaceNewNav};
`;

export function SuspenseGlobal() {
    return <FullPageBackground />;
}
