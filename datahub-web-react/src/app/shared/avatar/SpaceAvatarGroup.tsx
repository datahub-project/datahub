import { Avatar } from 'antd';
import styled from 'styled-components';

export const SpacedAvatarGroup = styled(Avatar.Group)`
    align-items: center;
    &&& > .ant-avatar-circle.ant-avatar {
        margin-right: 10px;
        height: 24px;
        width: 24px;
    }
    &&& > .ant-avatar-circle.ant-avatar .ant-avatar-string {
        top: -20%;
    }
`;
