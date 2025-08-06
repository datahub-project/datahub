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
