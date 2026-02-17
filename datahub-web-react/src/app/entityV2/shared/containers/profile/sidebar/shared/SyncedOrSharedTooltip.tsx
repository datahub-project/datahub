import { Typography } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import { ActionType } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';

const HeaderText = styled(Typography.Text)`
    color: ${(props) => props.theme.colors.bgSurfaceBrand};
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
    const theme = useTheme();
    return (
        <Container>
            <HeaderText>
                This represents the time that the entity was last {type === ActionType.SYNC ? 'synchronized' : 'shared'}
            </HeaderText>
            <Wrapper>
                <ColoredContainer backgroundColor={theme.colors.bgSurfaceSuccess} textColor={theme.colors.textSuccess}>
                    {action} within the <BoldText>past week </BoldText>
                </ColoredContainer>
                <ColoredContainer
                    backgroundColor={theme.colors.bgSurfaceWarning}
                    textColor={theme.colors.textError}
                >
                    {action} within the <BoldText>past month</BoldText>
                </ColoredContainer>
                <ColoredContainer backgroundColor={theme.colors.bgSurfaceError} textColor={theme.colors.textError}>
                    {action} <BoldText>more than a month ago</BoldText>
                </ColoredContainer>
            </Wrapper>
        </Container>
    );
};

export default SyncedOrSharedTooltip;
