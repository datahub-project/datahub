import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

export const LINEAGE_ANNOTATION_NODE = 'lineage-annotation-node';

const Container = styled.div`
    background-color: ${colors.gray[1500]};
    border-radius: 200px;
    color: ${colors.gray[1700]};
    padding: 4px 6px;
`;

interface AnnotationNodeData {
    label: string;
}

interface Props {
    data: AnnotationNodeData;
}

export default function AnnotationNode({ data }: Props) {
    return <Container>{data.label}</Container>;
}
