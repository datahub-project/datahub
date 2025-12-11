/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { OwnershipTypeEntity } from '@types';

const DescriptionText = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 400;
`;

type Props = {
    ownershipType: OwnershipTypeEntity;
};

export const DescriptionColumn = ({ ownershipType }: Props) => {
    const description = ownershipType?.info?.description || '';

    return <DescriptionText>{description}</DescriptionText>;
};
