import { Form } from 'antd';
import styled from 'styled-components';

const DEFAULT_ASTERICK_COLOR = '#F5222D';

export const RequiredFieldForm = styled(Form)<{ requiredColor?: string }>`
    && {
        .ant-form-item-label > label.ant-form-item-required::before {
            color: ${(props) =>
                props.requiredColor || DEFAULT_ASTERICK_COLOR}; /* Change 'red' to any color you prefer */
            content: '*'; /* Ensure the asterisk is always used */
        }
    }
`;
