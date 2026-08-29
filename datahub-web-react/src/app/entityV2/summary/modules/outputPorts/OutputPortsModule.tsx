import { InfiniteScrollList } from '@components';
import React from 'react';

import { useGetOutputPorts } from '@app/entityV2/summary/modules/outputPorts/useGetOutputPorts';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { DataHubPageModuleType, Entity } from '@types';

const DEFAULT_PAGE_SIZE = 10;

export default function OutputPortsModule(props: ModuleProps) {
    const { loading, fetchOutputPorts, total } = useGetOutputPorts(DEFAULT_PAGE_SIZE);

    if (total === 0) {
        return null;
    }

    return (
        <LargeModule {...props} loading={loading} dataTestId="output-ports-module">
            <InfiniteScrollList<Entity>
                fetchData={fetchOutputPorts}
                renderItem={(entity) => (
                    <EntityItem entity={entity} key={entity.urn} moduleType={DataHubPageModuleType.OutputPorts} />
                )}
                pageSize={DEFAULT_PAGE_SIZE}
                totalItemCount={total}
            />
        </LargeModule>
    );
}
