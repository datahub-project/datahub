import { Typography } from 'antd';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '../../../entityV2/shared/constants';
import colors from '../../../../alchemy-components/theme/foundations/colors';

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

export const FormStepsContainer = styled.div`
    background-color: white;
    padding: 20px 16px;
    height: 100%;
    border-radius: 8px;
    display: flex;
    flex-direction: column;
    gap: 20px;
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
`;

export const FormHeaderContainer = styled.div`
    padding: 10px 16px;
    display: flex;
    flex-direction: column;
    background-color: ${colors.violet[100]};
    border-radius: 10px 10px 0 0;
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
`;
