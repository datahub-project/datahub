import { Form } from 'antd';
import styled from 'styled-components';

export const RequiredFieldForm = styled(Form)<{ requiredColor?: string }>`
    && {
        .ant-form-item-label > label.ant-form-item-required::before {
            color: ${(props) => props.requiredColor || props.theme.colors.textError};
            content: '*';
        }
    }
`;
