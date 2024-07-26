import { Typography } from 'antd';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '../../../entityV2/shared/constants';
import { colors } from '../../../../alchemy-components';

export const HeaderContainer = styled.div`
    display: flex;
    height: 64px;
    width: 200px;
    padding: 12px 14px;
    background-color: white;
    border-radius: 8px;
`;

export const FlexBox = styled.div`
    display: flex;
    flex-direction: column;
`;

export const FieldLabel = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 500;
    color: ${colors.gray[600]};
    margin-bottom: 4px;
`;

export const FormStepsContainer = styled.div`
    background-color: white;
    padding: 20px 16px;
    height: 100%;
    border-radius: 8px;
    display: flex;
    flex-direction: column;
    gap: 10px;
`;

export const StepContainer = styled.div`
    display: flex;
    align-items: start;
    gap: 8px;
`;

export const StepIndicator = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
`;

export const StepName = styled(Typography.Text)<{ isActiveStep?: boolean }>`
    font-size: 14px;
    line-height: normal;
    font-weight: ${(props) => (props.isActiveStep ? '700' : '500')};
    color: ${(props) => (props.isActiveStep ? REDESIGN_COLORS.TITLE_PURPLE : REDESIGN_COLORS.BODY_TEXT_GREY)};
`;

export const StepsDivider = styled.div`
    height: 28px;
    width: 2px;
    border-radius: 1px;
    background-color: ${REDESIGN_COLORS.GREY_100};
    margin-top: 5px;
`;

export const LeftSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
    margin-bottom: 16px;
`;

export const HeaderText = styled(Typography.Text)`
    font-size: 16px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
`;

export const SubText = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.SUB_TEXT};
`;

export const StepNumber = styled.div`
    font-size: 12px;
    line-height: normal;
    font-weight: 700;
    color: ${REDESIGN_COLORS.GREY_300};
`;

export const NewFormContainer = styled.div`
    display: flex;
    gap: 16px;
    flex: 1;
`;

export const Body = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    background-color: white;
    border-radius: 8px;
`;

export const FormContentContainer = styled.div`
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 20px;
`;

export const FormHeaderContainer = styled.div`
    padding: 10px 16px;
    display: flex;
    background-color: #959fe0;
    border-radius: 8px 8px 0 0;
    gap: 10px;
    margin: 1px;
`;

export const FormHeaderText = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;

    p {
        line-height: normal;
    }
`;

export const FormColorIcon = styled.div`
    height: 40px;
    width: 40px;
    background-color: #aab5fc;
    border-radius: 12px;
`;

export const MainFormContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
    flex: 1;
    margin: 0 16px 16px 0;
`;

export const FormFooterContainer = styled.div`
    display: flex;
    background-color: white;
    border-radius: 8px;
    height: 62px;
    padding: 12px;
    justify-content: space-between;
`;

export const ButtonsContainer = styled.div`
    display: flex;
    gap: 12px;
`;

export const StepNameHeading = styled(Typography.Text)`
    font-size: 16px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
`;

export const StepDescription = styled(Typography.Text)`
    font-size: 12px;
    font-style: italic;
    font-weight: 500;
    color: ${REDESIGN_COLORS.BODY_TEXT_GREY};
`;

export const FormFieldsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 28px;
    max-width: 400px;
`;

export const NextStepText = styled(Typography.Text)`
    font-size: 10px;
    font-weight: 700;
    opacity: 0.6;
    color: ${REDESIGN_COLORS.GREY_300};
    line-height: normal;
`;
