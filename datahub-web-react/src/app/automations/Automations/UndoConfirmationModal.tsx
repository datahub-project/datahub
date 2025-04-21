import CloseOutlinedIcon from '@mui/icons-material/CloseOutlined';
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { ModalTitle, StyledButton, StyledModal } from '@app/shared/share/v2/styledComponents';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 5px;
`;

const Header = styled.div`
    font-size: 18px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
`;

const Content = styled.div`
    font-size: 16px;
    font-weight: 400;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
`;

const ButtonsContainer = styled.div`
    display: flex;
    gap: 10px;
    align-self: end;
    margin-bottom: 10px;
`;

interface Props {
    showUndoConfirmation: boolean;
    handleUndo: (e: any) => void;
    handleClose: () => void;
}

export const UndoConfirmationModal = ({ showUndoConfirmation, handleClose, handleUndo }: Props) => {
    const { theme } = useCustomTheme();

    return (
        <StyledModal
            open={showUndoConfirmation}
            onCancel={handleClose}
            footer={null}
            closeIcon={<CloseOutlinedIcon />}
            title={<ModalTitle>Are you sure?</ModalTitle>}
        >
            <Container>
                <Header>Undoing the Automation</Header>
                <Content>Are you sure you want to undo the automation?</Content>
                <ButtonsContainer>
                    <StyledButton $color={getColor('primary', 500, theme)} onClick={handleUndo}>
                        Yes
                    </StyledButton>
                    <StyledButton
                        $type="filled"
                        $color={getColor('primary', 500, theme)}
                        $hoverColor={REDESIGN_COLORS.HOVER_PURPLE}
                        onClick={handleClose}
                    >
                        No
                    </StyledButton>
                </ButtonsContainer>
            </Container>
        </StyledModal>
    );
};
