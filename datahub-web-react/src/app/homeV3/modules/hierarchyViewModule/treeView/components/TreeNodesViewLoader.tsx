/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Loader } from '@components';
import React from 'react';
import styled from 'styled-components';

import DepthMargin from '@app/homeV3/modules/hierarchyViewModule/treeView/DepthMargin';
import ExpandToggler from '@app/homeV3/modules/hierarchyViewModule/treeView/ExpandToggler';
import Row from '@app/homeV3/modules/hierarchyViewModule/treeView/components/Row';

const LoaderWrapper = styled.div``;

interface Props {
    depth: number;
}

export default function TreeNodesViewLoader({ depth }: Props) {
    return (
        <Row>
            <DepthMargin depth={depth + 1} />
            <ExpandToggler expandable={false} />
            <LoaderWrapper>
                <Loader size="xs" />
            </LoaderWrapper>
        </Row>
    );
}
