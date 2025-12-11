/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { EntityCardList } from '@app/homeV2/content/recent/EntityCardList';

// import styled from 'styled-components';
// import { BulbTwoTone } from '@ant-design/icons';
import { Entity } from '@types';

// const BULB_COLOR = '#EEAD1C';

// const StyledBulb = styled(BulbTwoTone)`
//     font-size: 20px;
// `;

type Props = {
    entities: Entity[];
};

// TODO: Decide whether we want the lightbulb or not.
export const RecentlyEditedOrViewed = ({ entities }: Props) => {
    return <EntityCardList title="You recently viewed" entities={entities} isHomePage />;
};
