import { Camera } from '@phosphor-icons/react/dist/csr/Camera';
import { toPng } from 'html-to-image';
import React, { useContext } from 'react';
import { useTranslation } from 'react-i18next';
import { getRectOfNodes, getTransformForBounds, useReactFlow, useStoreApi } from 'reactflow';
import { useTheme } from 'styled-components';

import LineageControlIcon from '@app/lineage/controls/LineageControlIcon';
import { LineageNodesContext } from '@app/lineageV3/common';
import { StyledPanelButton } from '@app/lineageV3/controls/StyledPanelButton';
import { downloadImage } from '@app/lineageV3/utils/lineageUtils';

type Props = {
    showExpandedText: boolean;
    isExpanded: boolean;
};

export default function DownloadLineageScreenshotButton({ showExpandedText, isExpanded }: Props) {
    const { t } = useTranslation('lineage');
    const themeConfig = useTheme();
    const { getNodes } = useReactFlow();
    const storeApi = useStoreApi();
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

        // Scope the viewport lookup to THIS React Flow instance. The page can contain
        // multiple `.react-flow__viewport` elements (e.g. the Summary tab keeps a hidden
        // LineageExplorer preview mounted), so a document-wide querySelector may grab the
        // wrong graph. The store's `domNode` is this instance's `.react-flow` wrapper.
        const viewport = storeApi.getState().domNode?.querySelector<HTMLElement>('.react-flow__viewport');
        if (!viewport) {
            // eslint-disable-next-line no-console
            console.error('Failed to capture lineage screenshot: react-flow viewport not found');
            return;
        }

        toPng(viewport, {
            backgroundColor: themeConfig.colors.bgSurface,
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
            $showText={isExpanded}
            onClick={() => {
                getPreviewImage();
            }}
        >
            <LineageControlIcon icon={Camera} color="icon" />
            {showExpandedText ? t('controls.screenshotButton.label') : null}
        </StyledPanelButton>
    );
}
