import React from 'react';
import styled from 'styled-components';
import { DeleteOutlined, MoreOutlined } from '@ant-design/icons';
import { Button, Dropdown, Menu, Typography } from 'antd';
import { DataHubViewType } from '../../../types.generated';
import { ANTD_GRAY } from '../shared/constants';
import { ViewTypeLabel } from './ViewTypeLabel';

const StyledDescription = styled.div`
    max-width: 300px;
`;

const ActionButtonsContainer = styled.div`
    display: flex;
    justify-content: right;
    align-items: center;
    padding-right: 8px;
`;

const EditButton = styled(Button)`
    margin-right: 16px;
`;

const StyledMenuButton = styled(Button)`
    && {
        padding: 0px;
        margin: 0px;
    }
`;

const StyledMoreOutlined = styled(MoreOutlined)`
    && {
        font-size: 20px;
    }
`;

type NameColumnProps = {
    name: string;
    record: any;
    onEditView: (urn) => void;
};

export function NameColumn({ name, record, onEditView }: NameColumnProps) {
    return (
        <Button type="text" onClick={() => onEditView(record.urn)}>
            <Typography.Text strong>{name}</Typography.Text>
        </Button>
    );
}

type DescriptionColumnProps = {
    description: string;
};

export function DescriptionColumn({ description }: DescriptionColumnProps) {
    return (
        <StyledDescription>
            {description || <Typography.Text type="secondary">No description</Typography.Text>}
        </StyledDescription>
    );
}

type ViewTypeColumnProps = {
    viewType: DataHubViewType;
};

export function ViewTypeColumn({ viewType }: ViewTypeColumnProps) {
    return <ViewTypeLabel color={ANTD_GRAY[8]} type={viewType} />;
}

type ActionColumnProps = {
    record: any;
    onEditView: (urn) => void;
    onDeleteView: (urn) => void;
};

export function ActionsColumn({ record, onEditView, onDeleteView }: ActionColumnProps) {
    return (
        <ActionButtonsContainer>
            <EditButton onClick={() => onEditView(record.urn)}>EDIT</EditButton>
            <Dropdown
                overlay={
                    <Menu>
                        <Menu.Item key="delete" onClick={() => onDeleteView(record.urn)}>
                            <span>
                                <DeleteOutlined /> Delete
                            </span>
                        </Menu.Item>
                    </Menu>
                }
            >
                <StyledMenuButton data-testid="views-table-dropdown" type="text">
                    <StyledMoreOutlined />
                </StyledMenuButton>
            </Dropdown>
        </ActionButtonsContainer>
    );
}
