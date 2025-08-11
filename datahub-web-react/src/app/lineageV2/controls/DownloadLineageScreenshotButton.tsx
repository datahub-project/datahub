import { CameraOutlined } from '@ant-design/icons';
import { toPng } from 'html-to-image';
import React, { useContext } from 'react';
import { getRectOfNodes, getTransformForBounds, useReactFlow } from 'reactflow';

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

export default function DownloadLineageScreenshotButton({ showExpandedText }: Props) {
    const { getNodes } = useReactFlow();
    const { rootUrn, nodes } = useContext(LineageNodesContext);

    const getPreviewImage = () => {
        const nodesBounds = getRectOfNodes(getNodes());
        const imageWidth = nodesBounds.width + 200;
        const imageHeight = nodesBounds.height + 200;
        const transform = getTransformForBounds(nodesBounds, imageWidth, imageHeight, 0.5, 2);

        // Get the entity name for the screenshot filename
        const rootEntity = nodes.get(rootUrn);
        const entityName = rootEntity?.entity?.name || 'lineage';
        // Clean the entity name to be safe for filename use
        const cleanEntityName = entityName.replace(/[^a-zA-Z0-9_-]/g, '_');

        toPng(document.querySelector('.react-flow__viewport') as HTMLElement, {
            backgroundColor: '#f8f8f8',
            width: imageWidth,
            height: imageHeight,
            style: {
                width: String(imageWidth),
                height: String(imageHeight),
                transform: `translate(${transform[0]}px, ${transform[1]}px) scale(${transform[2]})`,
            },
        }).then((dataUrl) => {
            downloadImage(dataUrl, cleanEntityName);
        });
    };

    return (
        <StyledPanelButton
            type="text"
            onClick={() => {
                getPreviewImage();
            }}
        >
            <CameraOutlined />
            {showExpandedText ? 'Screenshot' : null}
        </StyledPanelButton>
    );
}
