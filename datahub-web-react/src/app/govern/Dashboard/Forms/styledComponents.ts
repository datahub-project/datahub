import { colors } from '@src/alchemy-components';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { Modal, Spin, Tag, Typography } from 'antd';
import styled from 'styled-components';

export const CreateFormContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 20px;
    height: 100%;
`;

export const ContentContainer = styled.div`
    display: flex;
    flex-direction: column;
    padding: 16px;
    margin-right: 16px;
    border-radius: 12px;
    flex: 1;
    background-color: ${colors.white};
    gap: 12px;
    height: calc(100% - 76px);
    overflow: auto;
    box-shadow: 0px 0px 24px 0px rgba(0, 0, 0, 0.1);
`;

export const FooterContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 20px;
    margin-right: 16px;
    height: 64px;
    padding: 16px;
    background-color: ${colors.white};
    border-radius: 12px 12px 0px 0px;
    box-shadow: 0px 0px 24px 0px rgba(0, 0, 0, 0.1);
`;

export const AddElementContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

export const LeftSection = styled.div`
    display: flex;
    flex-direction: column;
`;

export const OwnershipCheckbox = styled.div`
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 10px 0;
`;

export const FormFieldsContainer = styled.div`
    display: flex;
    flex-direction: column;
    max-width: 412px;
    margin-top: 20px;
`;

export const FieldLabel = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 500;
    color: ${colors.gray[600]};
    margin-bottom: 4px;
`;

export const BreadcrumbContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 12px 0 16px 0;
`;

export const BackText = styled.div`
    :hover {
        cursor: pointer;
    }
`;

export const StyledSpin = styled(Spin)`
    max-height: 100% !important;
    color: ${colors.violet};
`;

export const SelectOptionContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin-top: 3px;
`;

export const OptionsContainer = styled.div`
    display: flex;
    flex-direction: column;
    padding: 12px;
    gap: 10px;
`;

export const StyledModal = styled(Modal)`
    font-family: Mulish;

    &&& .ant-modal-content {
        box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);
        border-radius: 12px;
        width: 452px;
    }

    .ant-modal-body {
        padding: 0 20px;
    }
`;

export const ModalFooter = styled.div`
    display: flex;
    gap: 16px;
    justify-content: end;
`;

export const QuestionsList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 10px;
    margin: 10px 0;
`;

export const CardContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    border: 1px solid ${colors.gray[100]};
    border-radius: 4px;
    padding: 20px;
`;

export const MandatoryTag = styled(Tag)`
    background-color: #f1fbfe;
    border: 1px solid #f1fbfe !important;
    color: #09739a;
    font-size: 14px;
    font-weight: 600;
    padding: 3px 10px;
`;

export const CardData = styled.div<{ width: string }>`
    width: ${(props) => props.width};
`;

export const CardIcons = styled.div`
    display: flex;
    justify-content: end;
    gap: 12px;

    div {
        border: 1px solid ${REDESIGN_COLORS.SILVER_GREY};
        border-radius: 20px;
        width: 28px;
        height: 28px;
        padding: 4px;
        color: ${REDESIGN_COLORS.GREY_300};
        :hover {
            cursor: pointer;
        }
    }
`;
