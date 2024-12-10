import React from 'react';
import styled from 'styled-components';
import { Collapse } from 'antd';

import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { applyOpacity } from '../../../../shared/styleUtils';
import { StructuredReportItemContext } from './StructuredReportItemContext';
import { StructuredReportLogEntry } from '../../types';

const StyledCollapse = styled(Collapse)<{ color: string }>`
    background-color: ${(props) => applyOpacity(props.color, 8)};
    border: 1px solid ${(props) => applyOpacity(props.color, 20)};
    display: flex;

    && {
        .ant-collapse-header {
            display: flex;
            align-items: center;
            overflow: auto;
        }

        .ant-collapse-item {
            border: none;
            width: 100%;
        }
    }
`;

const Item = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    gap: 4px;
`;

const Content = styled.div`
    border-radius: 8px;
`;

const Text = styled.div`
    display: flex;
    flex-direction: column;
`;

const Type = styled.div`
    font-weight: bold;
    font-size: 14px;
`;

const Message = styled.div`
    color: ${ANTD_GRAY[8]};
`;

interface Props {
    item: StructuredReportLogEntry;
    color: string;
    icon?: React.ComponentType<any>;
}

export function StructuredReportItem({ item, color, icon }: Props) {
    const Icon = icon;
    return (
        <StyledCollapse color={color}>
            <Collapse.Panel
                header={
                    <Item>
                        {Icon ? <Icon style={{ fontSize: 16, color, marginRight: 12 }} /> : null}
                        <Text>
                            <Type>{item.title}</Type>
                            <Message>{item.message}</Message>
                        </Text>
                    </Item>
                }
                key="0"
            >
                <Content>
                    <StructuredReportItemContext item={item} />
                </Content>
            </Collapse.Panel>
        </StyledCollapse>
    );
}
