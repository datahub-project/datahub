import React from 'react';
import styled from 'styled-components';

import { FilterRenderer } from '@app/searchV2/filters/render/FilterRenderer';
import { DeprecationFilter } from '@app/searchV2/filters/render/deprecation/DeprecationFilter';
import { FilterRenderProps } from '@app/searchV2/filters/render/types';

import DeprecatedIcon from '@images/deprecated-status.svg?react';

const StyledDeprecatedIcon = styled(DeprecatedIcon)`
    color: inherit;
    path {
        fill: currentColor;
    }
    && {
        fill: currentColor;
    }
    align-items: center;
`;

export class DeprecationRenderer implements FilterRenderer {
    field = 'deprecated';

    render = (props: FilterRenderProps) => <DeprecationFilter {...props} icon={this.icon()} />;

    icon = () => <StyledDeprecatedIcon />;

    valueLabel = (value: string) => {
        if (value === 'true') {
            return <>Is Deprecated</>;
        }
        return <>Is Not Deprecated</>;
    };
}
