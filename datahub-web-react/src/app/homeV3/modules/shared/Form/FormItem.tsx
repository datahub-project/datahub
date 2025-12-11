/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
