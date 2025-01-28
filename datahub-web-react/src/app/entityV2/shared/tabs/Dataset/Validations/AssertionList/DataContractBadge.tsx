import React from 'react';
import { AuditOutlined } from '@ant-design/icons';
import { REDESIGN_COLORS } from '@src/app/entity/shared/constants';
import { Tooltip } from '@components';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

const DataContractLogo = styled(AuditOutlined)`
    margin-left: 8px;
    font-size: 16px;
    color: ${REDESIGN_COLORS.BLUE};
`;
export const DataContractBadge = ({ link }: { link: string }) => {
    return (
        <Tooltip
            title={
                <>
                    Part of Data Contract{' '}
                    <Link to={link} style={{ color: REDESIGN_COLORS.BLUE }}>
                        view
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
