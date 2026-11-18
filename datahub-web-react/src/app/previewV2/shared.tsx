import { Button } from '@components';
import styled from 'styled-components';

export const ActionsAndStatusSection = styled.div`
    flex: 1 0 auto;

    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: flex-end;
    gap: 5px;
`;

// Hidden until the preview card is hovered; the card's container reveals it.
export const TransparentButton = styled(Button)`
    display: none;

    &:hover {
        display: inline-flex;
    }
`;
