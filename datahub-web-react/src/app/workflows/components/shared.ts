import { Form } from 'antd';
import styled from 'styled-components';

export const StyledFormItem = styled(Form.Item)`
    margin-bottom: 16px;

    .ant-form-item-required::before {
        color: red !important;
    }
`;
