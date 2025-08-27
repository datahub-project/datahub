import { Form } from 'antd';
import styled from 'styled-components';

const FormItem = styled(Form.Item)`
    margin-bottom: 0;

    .ant-form-item {
        margin-bottom: 0;
    }

    .ant-form-item-control-input {
        min-height: 0;
    }
`;

export default FormItem;
