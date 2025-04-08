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
    return <EntityCardList title="You recently viewed" entities={entities} />;
};
