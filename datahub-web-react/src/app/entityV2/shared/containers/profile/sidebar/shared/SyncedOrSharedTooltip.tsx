import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import { ActionType } from './utils';
import { REDESIGN_COLORS } from '../../../../constants';

const HeaderText = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.LIGHT_TEXT_DARK_BACKGROUND};
    font-size: 14px;
    font-weight: 500;
`;

const Container = styled.div`
    font-family: Mulish;
`;

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    margin: 12px 0;
`;

const ColoredContainer = styled.div<{ backgroundColor: string; textColor: string }>`
    border-radius: 20px;
    padding: 4px 12px;
    background-color: ${(props) => props.backgroundColor};
    color: ${(props) => props.textColor};
    font-weight: 500;
    width: max-content;
`;

const BoldText = styled(Typography.Text)`
    font-weight: 600;
`;

interface Props {
    type: ActionType;
}

const SyncedOrSharedTooltip = ({ type }: Props) => {
    const action = type === ActionType.SYNC ? 'Synced' : 'Shared';
    return (
        <Container>
            <HeaderText>
                This represents the time that the entity was last {type === ActionType.SYNC ? 'synchronized' : 'shared'}
            </HeaderText>
            <Wrapper>
                <ColoredContainer backgroundColor={REDESIGN_COLORS.GREEN_LIGHT} textColor={REDESIGN_COLORS.GREEN_800}>
                    {action} within the <BoldText>past week </BoldText>
                </ColoredContainer>
                <ColoredContainer
                    backgroundColor={REDESIGN_COLORS.YELLOW_BACKGROUND}
                    textColor={REDESIGN_COLORS.RED_800}
                >
                    {action} within the <BoldText>past month</BoldText>
                </ColoredContainer>
                <ColoredContainer backgroundColor={REDESIGN_COLORS.RED_LIGHT} textColor={REDESIGN_COLORS.RED_800}>
                    {action} <BoldText>more than a month ago</BoldText>
                </ColoredContainer>
            </Wrapper>
        </Container>
    );
};

export default SyncedOrSharedTooltip;
