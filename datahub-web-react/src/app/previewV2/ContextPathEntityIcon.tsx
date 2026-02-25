import { FolderOpenOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

const IconWrapper = styled.span`
    img,
    svg {
        height: 12px;
    }
`;

const DefaultIcon = styled(FolderOpenOutlined)`
    &&& {
        font-size: 14px;
    }
`;

function ContextPathEntityIcon() {
    // For now, we keep it simple - each parent shares the same icon within the context path.
    return (
        <IconWrapper>
            <DefaultIcon />
        </IconWrapper>
    );
}

export default ContextPathEntityIcon;
