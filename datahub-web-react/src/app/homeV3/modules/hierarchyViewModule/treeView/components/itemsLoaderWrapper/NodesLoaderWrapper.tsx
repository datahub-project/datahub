import React, { useMemo } from 'react';

import NodesLoaderByButtonWrapper from '@app/homeV3/modules/hierarchyViewModule/treeView/components/itemsLoaderWrapper/NodesLoaderByButtonWrapper';
import NodesLoaderInfiniteScrollWrapper from '@app/homeV3/modules/hierarchyViewModule/treeView/components/itemsLoaderWrapper/NodesLoaderInfiniteScrollWrapper';
import { NodesLoaderWrapperProps } from '@app/homeV3/modules/hierarchyViewModule/treeView/components/itemsLoaderWrapper/types';
import { LoadingTriggerType } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

interface Props extends NodesLoaderWrapperProps {
    trigger?: LoadingTriggerType;
    loading?: boolean;
}

export default function NodesLoaderWrapper({ children, trigger, ...props }: React.PropsWithChildren<Props>) {
    const NodesLoaderWrapperComponent = useMemo(() => {
        switch (trigger) {
            case 'button':
                return NodesLoaderByButtonWrapper;
            case 'infiniteScroll':
                return NodesLoaderInfiniteScrollWrapper;
            default:
                return NodesLoaderByButtonWrapper;
        }
    }, [trigger]);

    return <NodesLoaderWrapperComponent {...props}>{children}</NodesLoaderWrapperComponent>;
}
