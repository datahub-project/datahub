import React from 'react';
import styled, { useTheme } from 'styled-components';

import { ActionType } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';

const HeaderText = styled.span`
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

const ColoredContainer = styled.div<{ $bgColor: string; $textColor: string }>`
    border-radius: 20px;
    padding: 4px 12px;
    background-color: ${(props) => props.$bgColor};
    color: ${(props) => props.$textColor};
    font-weight: 500;
    width: max-content;
`;

const BoldText = styled.span`
    font-weight: 600;
`;

interface Props {
    type: ActionType;
}

const SyncedOrSharedTooltip = ({ type }: Props) => {
    const theme = useTheme();
    const action = type === ActionType.SYNC ? 'Synced' : 'Shared';
    return (
        <Container>
            <HeaderText>
                This represents the time that the entity was last {type === ActionType.SYNC ? 'synchronized' : 'shared'}
            </HeaderText>
            <Wrapper>
                <ColoredContainer
                    $bgColor={theme.colors.bgSurfaceSuccess}
                    $textColor={theme.colors.textOnSurfaceSuccess}
                >
                    {action} within the <BoldText>past week </BoldText>
                </ColoredContainer>
                <ColoredContainer
                    $bgColor={theme.colors.bgSurfaceWarning}
                    $textColor={theme.colors.textOnSurfaceWarning}
                >
                    {action} within the <BoldText>past month</BoldText>
                </ColoredContainer>
                <ColoredContainer $bgColor={theme.colors.bgSurfaceError} $textColor={theme.colors.textOnSurfaceError}>
                    {action} <BoldText>more than a month ago</BoldText>
                </ColoredContainer>
            </Wrapper>
        </Container>
    );
};

export default SyncedOrSharedTooltip;
