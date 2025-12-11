/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { TooltipPlacement } from 'antd/es/tooltip';
import React from 'react';

import { CompactEntityNameComponent } from '@app/recommendations/renderer/component/CompactEntityNameComponent';
import { Entity } from '@src/types.generated';

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
