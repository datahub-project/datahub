import { Popover, PopoverPlacement } from '@components';
import React from 'react';

import { PreviewContextProps } from '@app/entityV2/shared/PreviewContext';
import EntityHoverCard from '@app/sharedV2/hoverCard/EntityHoverCard';

import { Entity } from '@types';

type Props = {
    entity?: Entity;
    // whether the tooltip can be opened or if it should always stay closed
    canOpen?: boolean;
    children: React.ReactNode;
    placement?: PopoverPlacement;
    showArrow?: boolean;
    previewContext?: PreviewContextProps;
};

export const HoverEntityTooltip = ({
    entity,
    canOpen = true,
    children,
    placement,
    showArrow = false,
    previewContext,
}: Props) => {
    if (!entity || !entity.type || !entity.urn) {
        return <>{children}</>;
    }

    return (
        <Popover
            showArrow={showArrow}
            open={canOpen ? undefined : false}
            placement={placement || 'bottom'}
            content={<EntityHoverCard entity={entity} propagationDetails={previewContext?.propagationDetails} />}
            // Above the 1051 browser/modal wrappers a hover trigger can sit inside.
            zIndex={1100}
        >
            {children}
        </Popover>
    );
};
