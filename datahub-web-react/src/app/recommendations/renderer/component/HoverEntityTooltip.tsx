import { Tooltip } from 'antd';
import { TooltipPlacement } from 'antd/es/tooltip';
import React from 'react';
import { Entity } from '../../../../types.generated';
import { PreviewType } from '../../../entity/Entity';
import { useEntityRegistry } from '../../../useEntityRegistry';

type Props = {
    entity?: Entity;
    // whether the tooltip can be opened or if it should always stay closed
    canOpen?: boolean;
    children: React.ReactNode;
    placement?: TooltipPlacement;
    showArrow?: boolean;
    width?: number;
    maxWidth?: number;
};

export const HoverEntityTooltip = ({
    entity,
    canOpen = true,
    children,
    placement,
    showArrow = false,
    width = 360,
    maxWidth = 450,
}: Props) => {
    const entityRegistry = useEntityRegistry();

    if (!entity || !entity.type || !entity.urn) {
        return <>{children}</>;
    }

    return (
        <Tooltip
            showArrow={showArrow}
            open={canOpen ? undefined : false}
            color="white"
            placement={placement || 'bottom'}
            overlayStyle={{ minWidth: width, maxWidth }}
            overlayInnerStyle={{ padding: 20, borderRadius: 12, overflow: 'hidden', position: 'relative' }}
            title={entityRegistry.renderPreview(entity.type, PreviewType.HOVER_CARD, entity)}
            zIndex={1000}
        >
            {children}
        </Tooltip>
    );
};
