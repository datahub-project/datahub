/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components/macro';

import { EntityCard } from '@app/homeV2/content/recent/EntityCard';

import { Entity } from '@types';

const StyledEntityCard = styled(EntityCard)`
    min-width: 150px;
    max-width: 220px;
`;

interface Props {
    entity: Entity;
}

export default function SummaryEntityCard({ entity }: Props) {
    return <StyledEntityCard entity={entity} />;
}
