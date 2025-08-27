import { Tooltip, colors } from '@components';
import { TooltipPlacement } from 'antd/es/tooltip';
import React from 'react';

import { PreviewType } from '@app/entity/Entity';
import { PreviewContext } from '@app/entityV2/Entity';
import { HoverEntityTooltipContext } from '@app/recommendations/HoverEntityTooltipContext';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Entity } from '@types';

type Props = {
    entity?: Entity;
    // whether the tooltip can be opened or if it should always stay closed
    canOpen?: boolean;
    children: React.ReactNode;
    placement?: TooltipPlacement;
    showArrow?: boolean;
    width?: number;
    maxWidth?: number;
    entityCount?: number;
    previewContext?: PreviewContext;
};

export const HoverEntityTooltip = ({
    entity,
    canOpen = true,
    children,
    placement,
    showArrow = false,
    width = 360,
    maxWidth = 500,
    entityCount = undefined,
    previewContext,
}: Props) => {
    const entityRegistry = useEntityRegistryV2();

    if (!entity || !entity.type || !entity.urn) {
        return <>{children}</>;
    }

    const clampToViewportWidth = (value: number | string) => {
        // defensive programming in case HoverEntityTooltip ever starts allowing
        // maxWidth as a string.
        return `min(100vw, ${value}${typeof value === 'number' ? 'px' : ''})`;
    };

    return (
        <HoverEntityTooltipContext.Provider value={{ entityCount }}>
            <Tooltip
                showArrow={showArrow}
                open={canOpen ? undefined : false}
                color="white"
                placement={placement || 'bottom'}
                overlayStyle={{ minWidth: width, maxWidth: clampToViewportWidth(maxWidth), zIndex: 1100 }}
                overlayInnerStyle={{
                    padding: 16,
                    borderRadius: 12,
                    overflow: 'hidden',
                    position: 'relative',
                    color: colors.gray[1700],
                }}
                title={entityRegistry.renderPreview(
                    entity.type,
                    PreviewType.HOVER_CARD,
                    entity,
                    undefined,
                    previewContext,
                )}
                zIndex={1000}
            >
                {children}
            </Tooltip>
        </HoverEntityTooltipContext.Provider>
    );
};
