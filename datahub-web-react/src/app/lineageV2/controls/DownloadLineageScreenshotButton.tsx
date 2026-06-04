import { CameraOutlined } from '@ant-design/icons';
import { message } from 'antd';
import { toPng } from 'html-to-image';
import React, { useContext, useState } from 'react';
import { getRectOfNodes, getTransformForBounds, useReactFlow } from 'reactflow';
import { useTheme } from 'styled-components';

import LineageVisualizationContext from '@app/lineageV2/LineageVisualizationContext';
import { LineageNodesContext } from '@app/lineageV2/common';
import { StyledPanelButton } from '@app/lineageV2/controls/StyledPanelButton';

type Props = {
    showExpandedText: boolean;
};

function downloadImage(dataUrl: string, name?: string) {
    const now = new Date();
    const dateStr = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(
        now.getDate(),
    ).padStart(2, '0')}`;

    const timeStr = `${String(now.getHours()).padStart(2, '0')}${String(now.getMinutes()).padStart(2, '0')}${String(
        now.getSeconds(),
    ).padStart(2, '0')}`;

    const fileNamePrefix = name ? `${name}_` : 'reactflow_';
    const fileName = `${fileNamePrefix}${dateStr}_${timeStr}.png`;

    const a = document.createElement('a');
    a.setAttribute('download', fileName);
    a.setAttribute('href', dataUrl);
    a.click();
}

// Two RAFs + a layout-flush tail. First RAF runs after the render commit,
// second after React Flow's post-commit measurement pass. RAFs (not setTimeout)
// pace correctly when the tab is unfocused.
function nextTwoFrames(): Promise<void> {
    return new Promise((resolve) => {
        requestAnimationFrame(() => {
            requestAnimationFrame(() => {
                setTimeout(resolve, 50);
            });
        });
    });
}

export default function DownloadLineageScreenshotButton({ showExpandedText }: Props) {
    const themeConfig = useTheme();
    const { getNodes } = useReactFlow();
    const { rootUrn, nodes } = useContext(LineageNodesContext);
    const { setForceMountAll } = useContext(LineageVisualizationContext);
    const [isCapturing, setIsCapturing] = useState(false);

    const getPreviewImage = async () => {
        if (isCapturing) return;
        setIsCapturing(true);
        // Suspend virt for the capture: html-to-image walks the live DOM, so
        // with virt on it would only see the on-screen subset.
        setForceMountAll(true);
        try {
            await nextTwoFrames();

            const nodesBounds = getRectOfNodes(getNodes());
            const imageWidth = nodesBounds.width + 200;
            const imageHeight = nodesBounds.height + 200;
            const transform = getTransformForBounds(nodesBounds, imageWidth, imageHeight, 0.5, 2);

            const rootEntity = nodes.get(rootUrn);
            const entityName = rootEntity?.entity?.name || 'lineage';
            const cleanEntityName = entityName.replace(/[^a-zA-Z0-9_-]/g, '_');

            const dataUrl = await toPng(document.querySelector('.react-flow__viewport') as HTMLElement, {
                backgroundColor: themeConfig.colors.bgSurface,
                width: imageWidth,
                height: imageHeight,
                style: {
                    width: String(imageWidth),
                    height: String(imageHeight),
                    transform: `translate(${transform[0]}px, ${transform[1]}px) scale(${transform[2]})`,
                },
            });
            downloadImage(dataUrl, cleanEntityName);
        } finally {
            setForceMountAll(false);
            setIsCapturing(false);
        }
    };

    return (
        <StyledPanelButton
            type="text"
            disabled={isCapturing}
            onClick={() => {
                getPreviewImage().catch(() => {
                    message.error('Failed to capture lineage screenshot.');
                });
            }}
        >
            <CameraOutlined />
            {showExpandedText ? 'Screenshot' : null}
        </StyledPanelButton>
    );
}
