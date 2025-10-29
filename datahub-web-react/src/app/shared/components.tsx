import { Button } from 'antd';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

export const BaseButton = styled(Button)`
    &&& {
        display: flex;
        align-items: center;
        justify-content: center;
        border: none;
        box-shadow: none;
        border-radius: 50%;
    }
`;

export const RotatingButton = styled(BaseButton)<{ deg: number }>`
    transform: rotate(${(props) => props.deg}deg);
    transition: transform 250ms;
`;

export const BodyGridExpander = styled.div<{ isOpen: boolean }>`
    display: grid;
    grid-template-rows: ${(props) => (props.isOpen ? '1fr' : '0fr')};
    transition: grid-template-rows 250ms;
    overflow: hidden;
`;

export const BodyContainer = styled.div`
    min-height: 0;
`;

export const WhiteButton = styled(Button)`
    background-color: white;
    color: ${(props) => props.theme.styles['primary-color']};
    text-shadow: none;
`;
