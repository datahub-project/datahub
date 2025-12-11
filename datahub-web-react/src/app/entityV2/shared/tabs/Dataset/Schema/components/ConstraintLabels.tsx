/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { blue, green } from '@ant-design/colors';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

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
