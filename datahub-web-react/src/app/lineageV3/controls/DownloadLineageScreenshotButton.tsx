import { CameraOutlined } from '@ant-design/icons';
import { toPng } from 'html-to-image';
import React, { useContext } from 'react';
import { getRectOfNodes, getTransformForBounds, useReactFlow } from 'reactflow';

import { LineageNodesContext } from '@app/lineageV3/common';
import { StyledPanelButton } from '@app/lineageV3/controls/StyledPanelButton';
import { downloadImage } from '@app/lineageV3/utils/lineageUtils';

type Props = {
    showExpandedText: boolean;
};

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
