/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Alert, Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

const StyledAlert = styled(Alert)`
    white-space: normal;
`;

type Props = {
    onClickRetry?: () => void;
};

const SidebarLoadingError = ({ onClickRetry }: Props) => {
    return (
        <StyledAlert
            message="The sidebar failed to load."
            showIcon
            type="error"
            action={
                onClickRetry && (
                    <Button size="small" danger onClick={onClickRetry}>
                        Retry
                    </Button>
                )
            }
        />
    );
};

export default SidebarLoadingError;
