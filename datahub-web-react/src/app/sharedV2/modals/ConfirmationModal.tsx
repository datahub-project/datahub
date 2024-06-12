import React from 'react';
import styled from 'styled-components';
import CloseOutlinedIcon from '@mui/icons-material/CloseOutlined';
import { ModalTitle, StyledButton, StyledModal } from '../../shared/share/v2/styledComponents';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 5px;
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
    margin: 10px 0;
`;

interface Props {
    isOpen: boolean;
    handleConfirm: (e: any) => void;
    handleClose: () => void;
    modalTitle?: string;
    modalText?: string;
    isDeleteModal?: boolean;
}

export const ConfirmationModal = ({
    isOpen,
    handleClose,
    handleConfirm,
    modalTitle,
    modalText,
    isDeleteModal,
}: Props) => {
    return (
        <StyledModal
            open={isOpen}
            onCancel={handleClose}
            footer={null}
            closeIcon={<CloseOutlinedIcon />}
            title={<ModalTitle>{modalTitle || 'Confirm'}</ModalTitle>}
        >
            <Container>
                <Content>{modalText || 'Are you sure?'}</Content>
                <ButtonsContainer>
                    <StyledButton
                        $color={REDESIGN_COLORS.TITLE_PURPLE}
                        $type={!isDeleteModal ? 'filled' : 'outlined'}
                        onClick={handleConfirm}
                    >
                        Yes
                    </StyledButton>
                    <StyledButton
                        $type={isDeleteModal ? 'filled' : 'outlined'}
                        $color={REDESIGN_COLORS.TITLE_PURPLE}
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
