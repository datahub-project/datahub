import React from 'react';
// import styled from 'styled-components';
// import { BulbTwoTone } from '@ant-design/icons';
import { Entity } from '../../../../types.generated';
import { EntityCardList } from './EntityCardList';

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
