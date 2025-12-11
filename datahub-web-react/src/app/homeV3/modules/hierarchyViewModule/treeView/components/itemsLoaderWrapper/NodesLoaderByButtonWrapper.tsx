/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button } from '@components';
import React, { useMemo } from 'react';

import DepthMargin from '@app/homeV3/modules/hierarchyViewModule/treeView/DepthMargin';
import ExpandToggler from '@app/homeV3/modules/hierarchyViewModule/treeView/ExpandToggler';
import Row from '@app/homeV3/modules/hierarchyViewModule/treeView/components/Row';
import { NodesLoaderWrapperProps } from '@app/homeV3/modules/hierarchyViewModule/treeView/components/itemsLoaderWrapper/types';

export default function NodesLoaderByButtonWrapper({
    children,
    total,
    current,
    pageSize,
    depth,
    enabled,
    loading,
    onLoad,
}: React.PropsWithChildren<NodesLoaderWrapperProps>) {
    const loadMoreNumber = useMemo(() => Math.min(total - current, pageSize), [total, current, pageSize]);

    return (
        <>
            {children}

            {enabled && !loading && loadMoreNumber > 0 && (
                <Row>
                    <DepthMargin depth={depth + 1} />
                    <ExpandToggler expandable={false} />
                    <Button onClick={onLoad} variant="link" color="gray">
                        Show {loadMoreNumber} more
                    </Button>
                </Row>
            )}
        </>
    );
}
