import React from 'react';
import styled from 'styled-components';
import { blue, green } from '@ant-design/colors';
import { ANTD_GRAY } from '../../../../constants';

const Pill = styled.div`
    background-color: ${ANTD_GRAY[1]};
    border-radius: 10px;
    border: 1px solid;
    font-size: 12px;
    font-weight: 400;

    padding: 0 8px;
`;

const PrimaryKeyPill = styled(Pill)`
    color: ${blue[5]} !important;
    border-color: ${blue[2]};
`;

const ForeignKeyPill = styled(Pill)`
    color: ${green[5]} !important;
    border-color: ${green[2]};
`;

const NullablePill = styled(Pill)`
    color: ${ANTD_GRAY[7]} !important;
    border-color: ${ANTD_GRAY[7]};
`;

export function PrimaryKeyLabel() {
    return <PrimaryKeyPill>Primary Key</PrimaryKeyPill>;
}

export function ForeignKeyLabel() {
    return <ForeignKeyPill>Foreign Key</ForeignKeyPill>;
}

export function PartitioningKeyLabel() {
    return <PrimaryKeyPill>Partition Key</PrimaryKeyPill>;
}

export default function NullableLabel() {
    return <NullablePill>Nullable</NullablePill>;
}
