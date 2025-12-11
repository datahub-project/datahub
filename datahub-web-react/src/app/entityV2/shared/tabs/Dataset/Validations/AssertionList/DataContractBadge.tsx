/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { AuditOutlined } from '@ant-design/icons';
import { Tooltip, colors } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

const DataContractLogo = styled(AuditOutlined)`
    margin-left: 8px;
    font-size: 16px;
    color: ${(props) => props.theme.styles['primary-color']};
`;

export const DataContractBadge = ({ link }: { link: string }) => {
    return (
        <Tooltip
            title={
                <>
                    Part of Data Contract{' '}
                    <Link to={link} style={{ color: colors.gray[300] }}>
                        View
                    </Link>
                </>
            }
        >
            <Link to={link}>
                <DataContractLogo />
            </Link>
        </Tooltip>
    );
};
