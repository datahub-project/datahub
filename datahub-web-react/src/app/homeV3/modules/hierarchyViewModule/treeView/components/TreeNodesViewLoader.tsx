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
