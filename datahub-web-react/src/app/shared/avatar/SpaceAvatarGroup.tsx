/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Avatar } from 'antd';
import styled from 'styled-components';

export const SpacedAvatarGroup = styled(Avatar.Group)`
    &&& > .ant-avatar-circle.ant-avatar {
        margin-right: 10px;
        height: 24px;
        width: 24px;
    }
    &&& > .ant-avatar-circle.ant-avatar .ant-avatar-string {
        top: -20%;
    }
`;
