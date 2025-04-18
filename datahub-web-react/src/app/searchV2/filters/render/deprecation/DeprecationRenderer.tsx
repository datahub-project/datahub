import React from 'react';
import styled from 'styled-components';
import DeprecatedIcon from '../../../../../images/deprecated-status.svg?react';
import { FilterRenderer } from '../FilterRenderer';
import { FilterRenderProps } from '../types';
import { DeprecationFilter } from './DeprecationFilter';

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
