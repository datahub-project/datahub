import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import React from 'react';

import { FilterRenderer } from '@app/searchV2/filters/render/FilterRenderer';
import { HasActiveIncidentsFilter } from '@app/searchV2/filters/render/incident/HasActiveIncidentsFilter';
import { FilterRenderProps } from '@app/searchV2/filters/render/types';

export class HasActiveIncidentsRenderer implements FilterRenderer {
    field = 'hasActiveIncidents';

    name = 'Incidents';

    render = (props: FilterRenderProps) => <HasActiveIncidentsFilter {...props} />;

    icon = () => <Warning size={14} />;

    valueLabel = (value: string) => {
        if (value === 'true') {
            return <>Has Active Incidents</>;
        }
        return <>Has Resolved Incidents</>;
    };
}
