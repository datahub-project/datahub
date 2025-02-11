import { Entity } from '@src/types.generated';
import { TooltipPlacement } from 'antd/es/tooltip';
import React from 'react';
import { CompactEntityNameComponent } from './CompactEntityNameComponent';

type CompactEntityNameListProps = {
    entities: Array<Entity>;
    onClick?: (index: number) => void;
    linkUrlParams?: Record<string, string | boolean>;
    showFullTooltips?: boolean;
    showArrows?: boolean;
    placement?: TooltipPlacement;
};

export const CompactEntityNameList = ({
    entities,
    onClick,
    linkUrlParams,
    showFullTooltips = false,
    showArrows,
    placement,
}: CompactEntityNameListProps) => {
    return (
        <>
            {entities.map((entity, index) => (
                <CompactEntityNameComponent
                    key={entity?.urn || index}
                    entity={entity}
                    showFullTooltip={showFullTooltips}
                    showArrow={showArrows && index !== entities.length - 1}
                    placement={placement}
                    onClick={() => onClick?.(index)}
                    linkUrlParams={linkUrlParams}
                />
            ))}
        </>
    );
};
