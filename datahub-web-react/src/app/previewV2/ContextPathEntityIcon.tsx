/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
