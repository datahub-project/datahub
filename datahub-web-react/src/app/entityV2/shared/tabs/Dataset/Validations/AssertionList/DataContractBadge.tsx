import { AuditOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

const DataContractLogo = styled(AuditOutlined)`
    margin-left: 8px;
    font-size: 16px;
    color: ${(props) => props.theme.styles['primary-color']};
`;

export const DataContractBadge = ({ link }: { link: string }) => {
    const theme = useTheme();
    return (
        <Tooltip
            title={
                <>
                    Part of Data Contract{' '}
                    <Link to={link} style={{ color: theme.colors.icon }}>
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
