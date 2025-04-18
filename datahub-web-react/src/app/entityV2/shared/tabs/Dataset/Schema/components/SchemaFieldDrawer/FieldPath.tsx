import { EnterOutlined } from '@ant-design/icons';
import { Divider, Typography } from 'antd';
import { Popover } from '@components';
import React from 'react';
import styled from 'styled-components';
import RowIcon from '../../../../../../../../images/row-icon.svg?react';
import { REDESIGN_COLORS } from '../../../../../constants';

const FieldPathWrapper = styled.div`
    display: flex;
    align-self: center;
    stroke: ${REDESIGN_COLORS.WHITE};
`;

const PopoverContentWrapper = styled.span`
    color: ${REDESIGN_COLORS.BLACK};
`;
const FieldName = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 400;
    line-height: 24px;
    // padding-left: 15px;
    overflow: hidden;
    display: block;
    cursor: pointer;
`;
const LevelNum = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 400;
    line-height: 24px;
`;

const StyledDivider = styled(Divider)`
    margin: 5px;
    margin-left: 0px;
    height: 1px;
    background: ${REDESIGN_COLORS.BLACK};
    opacity: 0.1;
`;
const RowIconContainer = styled.div`
    position: relative;
    display: flex;
    align-items: center;
`;
const DepthContainer = styled.div`
    height: 13px;
    width: 13px;
    border-radius: 50%;
    background: ${REDESIGN_COLORS.WHITE};
    margin-left: -7px;
    margin-top: -12px;
    display: flex;
    align-items: center;
`;

const DepthNumber = styled(Typography.Text)`
    margin-left: 4px;
    background: transparent;
    color: ${REDESIGN_COLORS.PRIMARY_PURPLE};
    font-size: 10px;
    font-weight: 400;
`;

interface Props {
    displayName: string;
    setExpandedDrawerFieldPath: (fieldPath: string | null) => void;
}

export default function FieldPath({ displayName, setExpandedDrawerFieldPath }: Props) {
    const displayNameTokens = displayName.split('.');
    const isNestedField = displayNameTokens.length > 1;
    if (!isNestedField) {
        return null;
    }
    const content = (
        <PopoverContentWrapper>
            <LevelNum>{displayNameTokens.length} levels nested </LevelNum>
            <StyledDivider />
            {displayNameTokens.map((token, idx) => (
                <FieldName onClick={() => setExpandedDrawerFieldPath(displayNameTokens.slice(0, idx + 1).join('.'))}>
                    {idx !== 0 && (
                        <EnterOutlined
                            style={{
                                transform: 'scaleX(-1)',
                                paddingLeft: 6,
                                fontSize: 14,
                                fontWeight: 'bolder',
                            }}
                        />
                    )}
                    {idx === displayNameTokens.length - 1 ? <strong>{token}</strong> : token}
                </FieldName>
            ))}
        </PopoverContentWrapper>
    );
    return (
        <FieldPathWrapper>
            <Popover
                content={content}
                overlayStyle={{ borderRadius: 10, minWidth: 200 }}
                overlayInnerStyle={{ borderRadius: 10, border: '1px solid #D0DDE0', background: '#F5F9FA' }}
            >
                <RowIconContainer className="row-icon">
                    <RowIcon height={16} width={16} />
                    <DepthContainer className="depth-container">
                        <DepthNumber className="depth-text">{displayNameTokens.length}</DepthNumber>
                    </DepthContainer>
                </RowIconContainer>
            </Popover>
        </FieldPathWrapper>
    );
}
