import { Tooltip } from '@components';
import { TooltipPlacement } from 'antd/es/tooltip';
import React from 'react';
import { Entity } from '../../../../types.generated';
import { PreviewType } from '../../../entity/Entity';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useEmbeddedProfileLinkProps } from '../../../shared/useEmbeddedProfileLinkProps';
import { HoverEntityTooltipContext } from '../../HoverEntityTooltipContext';

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
};

export const HoverEntityTooltip = ({
    entity,
    canOpen = true,
    children,
    placement,
    showArrow,
    width = 360,
    maxWidth = 450,
    entityCount = undefined,
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const linkProps = useEmbeddedProfileLinkProps();

    if (!entity || !entity.type || !entity.urn) {
        return <>{children}</>;
    }

    const url = entityRegistry.getEntityUrl(entity.type, entity.urn);
    return (
        <HoverEntityTooltipContext.Provider value={{ entityCount }}>
            <Tooltip
                showArrow={showArrow}
                open={canOpen ? undefined : false}
                color="white"
                placement={placement || 'bottom'}
                overlayStyle={{ minWidth: width, maxWidth, zIndex: 1100 }}
                overlayInnerStyle={{ padding: 20, borderRadius: 20, overflow: 'hidden', position: 'relative' }}
                title={
                    <a href={url} {...linkProps}>
                        {entityRegistry.renderPreview(entity.type, PreviewType.HOVER_CARD, entity)}
                    </a>
                }
                zIndex={1000}
            >
                {children}
            </Tooltip>
        </HoverEntityTooltipContext.Provider>
    );
};
