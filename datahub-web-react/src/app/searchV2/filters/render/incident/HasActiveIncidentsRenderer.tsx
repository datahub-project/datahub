import React from 'react';
import { WarningOutlined } from '@ant-design/icons';
import { FilterRenderer } from '../FilterRenderer';
import { FilterRenderProps } from '../types';
import { HasActiveIncidentsFilter } from './HasActiveIncidentsFilter';

export class HasActiveIncidentsRenderer implements FilterRenderer {
    field = 'hasActiveIncidents';

    render = (props: FilterRenderProps) => <HasActiveIncidentsFilter {...props} icon={this.icon()} />;

    icon = () => <WarningOutlined />;

    valueLabel = (value: string) => {
        if (value === 'true') {
            return <>Has Active Incidents</>;
        }
        return <>Has Resolved Incidents</>;
    };
}
