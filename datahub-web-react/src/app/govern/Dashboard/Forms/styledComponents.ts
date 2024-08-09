import { colors } from '@src/alchemy-components';
import { Spin, Typography } from 'antd';
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
    max-width: 400px;
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
