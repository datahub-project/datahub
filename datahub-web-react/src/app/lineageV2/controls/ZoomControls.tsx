import { ZoomInOutlined, ZoomOutOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import { Panel, useReactFlow } from 'reactflow';
import styled from 'styled-components';
import { TRANSITION_DURATION_MS } from '../common';

const StyledZoomButton = styled(Button)`
    border-radius: 8px;
    border: 1px solid #00000015;
    box-shadow: 0px 2px 0px 0px rgba(0, 0, 0, 0.02);
    height: 40px;
    width: 40px;
    margin-bottom: 8px;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    display: flex;
    &:focus {
        color: unset;
        border-color: #00000015;
    }
`;

const ZoomControls: React.FC = () => {
    const { zoomIn, zoomOut } = useReactFlow();

    return (
        <Panel position="bottom-left">
            <StyledZoomButton tabIndex={-1} onClick={() => zoomIn({ duration: TRANSITION_DURATION_MS })}>
                <ZoomInOutlined />
            </StyledZoomButton>
            <StyledZoomButton tabIndex={-1} onClick={() => zoomOut({ duration: TRANSITION_DURATION_MS })}>
                <ZoomOutOutlined />
            </StyledZoomButton>
        </Panel>
    );
};

export default ZoomControls;
