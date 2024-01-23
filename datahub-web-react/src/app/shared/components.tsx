import { Button } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../entity/shared/constants';

export const HomePageButton = styled(Button)`
    margin-right: 12px;
    margin-left: 12px;
    margin-bottom: 12px;
    width: 160px;
    height: 140px;
    display: flex;
    justify-content: center;
    border-radius: 4px;
    align-items: center;
    flex-direction: column;
    border: 1px solid ${ANTD_GRAY[4]};
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    &&:hover {
        box-shadow: ${(props) => props.theme.styles['box-shadow-hover']};
    }
    white-space: unset;
`;

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
