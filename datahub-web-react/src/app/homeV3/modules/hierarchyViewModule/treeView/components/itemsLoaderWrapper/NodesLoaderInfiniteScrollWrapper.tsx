/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useEffect, useMemo, useState } from 'react';
import { useInView } from 'react-intersection-observer';
import styled from 'styled-components';

import { NodesLoaderWrapperProps } from '@app/homeV3/modules/hierarchyViewModule/treeView/components/itemsLoaderWrapper/types';

export const ObserverContainer = styled.div`
    height: 1px;
    margin-top: 1px;
`;

export default function NodesLoaderInfiniteScrollWrapper({
    children,
    total,
    current,
    enabled,
    loading,
    onLoad,
}: React.PropsWithChildren<NodesLoaderWrapperProps>) {
    const [scrollRef, inView] = useInView();
    // FYI: additional flag to prevent `onLoad` calling
    // when infinite scroll triggered but real loading hasn't started yet
    const [shouldPreventLoading, setShouldPreventLoading] = useState<boolean>(false);
    // reset flag when observer container is out of view (it's hidden while real loading because `loading` is true)
    useEffect(() => {
        if (!inView) setShouldPreventLoading(false);
    }, [inView]);

    const hasMoreNodes = useMemo(() => total - current > 0, [total, current]);

    useEffect(() => {
        if (enabled && inView && hasMoreNodes && !shouldPreventLoading) {
            setShouldPreventLoading(true);
            onLoad();
        }
    }, [shouldPreventLoading, inView, enabled, hasMoreNodes, onLoad]);

    return (
        <>
            {children}

            {enabled && !loading && hasMoreNodes && <ObserverContainer ref={scrollRef} />}
        </>
    );
}
