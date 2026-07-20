import { Button } from '@components';
import { MagnifyingGlassMinus } from '@phosphor-icons/react/dist/csr/MagnifyingGlassMinus';
import { MagnifyingGlassPlus } from '@phosphor-icons/react/dist/csr/MagnifyingGlassPlus';
import React from 'react';
import { Panel, useReactFlow } from 'reactflow';
import styled from 'styled-components';

import LineageControlIcon from '@app/lineage/controls/LineageControlIcon';
import { TRANSITION_DURATION_MS } from '@app/lineageV2/common';

const StyledZoomButton = styled(Button).attrs({
    variant: 'outline',
})`
    border-radius: 8px;
    border: 1px solid ${(props) => props.theme.colors.border};
    background-color: ${(props) => props.theme.colors.bg};
    color: ${(props) => props.theme.colors.icon};
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    height: 40px;
    width: 40px;
    margin-bottom: 8px;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    display: flex;
    &:hover {
        color: ${(props) => props.theme.colors.iconHover};
        border-color: ${(props) => props.theme.colors.borderHover};
    }
    &:focus {
        color: unset;
        border-color: ${(props) => props.theme.colors.border};
    }
`;

const ZoomControls: React.FC = () => {
    const { zoomIn, zoomOut } = useReactFlow();

    return (
        <Panel position="bottom-left">
            <StyledZoomButton tabIndex={-1} onClick={() => zoomIn({ duration: TRANSITION_DURATION_MS })}>
                <LineageControlIcon icon={MagnifyingGlassPlus} color="icon" />
            </StyledZoomButton>
            <StyledZoomButton tabIndex={-1} onClick={() => zoomOut({ duration: TRANSITION_DURATION_MS })}>
                <LineageControlIcon icon={MagnifyingGlassMinus} color="icon" />
            </StyledZoomButton>
        </Panel>
    );
};

export default ZoomControls;
