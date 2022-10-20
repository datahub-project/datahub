import { Tooltip } from 'antd';
import React from 'react';
import { Entity } from '../../../../types.generated';
import { PreviewType } from '../../../entity/Entity';
import { useEntityRegistry } from '../../../useEntityRegistry';

type Props = {
    entity?: Entity;
    // whether the tooltip can be opened or if it should always stay closed
    canOpen?: boolean;
    children: React.ReactNode;
};

export const HoverEntityTooltip = ({ entity, canOpen = true, children }: Props) => {
    const entityRegistry = useEntityRegistry();

    if (!entity || !entity.type || !entity.urn) {
        return <>{children}</>;
    }

    const url = entityRegistry.getEntityUrl(entity.type, entity.urn);
    return (
        <Tooltip
            visible={canOpen ? undefined : false}
            color="white"
            placement="topRight"
            overlayStyle={{ minWidth: 300, maxWidth: 500, width: 'fit-content' }}
            overlayInnerStyle={{ padding: 12 }}
            title={<a href={url}>{entityRegistry.renderPreview(entity.type, PreviewType.HOVER_CARD, entity)}</a>}
        >
            {children}
        </Tooltip>
    );
};
