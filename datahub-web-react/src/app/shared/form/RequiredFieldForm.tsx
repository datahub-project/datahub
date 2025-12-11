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
