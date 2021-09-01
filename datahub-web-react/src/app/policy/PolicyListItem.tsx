import React from 'react';
import { Button, List, Row, Space, Tag, Typography } from 'antd';
import { Policy, PolicyState } from '../../types.generated';

type Props = {
    policy: Policy;
    onView: () => void;
};

const inactiveTextColor = '#808080';

// TODO: Cleanup the styling.
export default function PolicyListItem({ policy, onView }: Props) {
    const isActive = policy.state === PolicyState.Active;
    const isEditable = policy.editable;
    const titleColor = isEditable ? undefined : inactiveTextColor;

    const policyPreview = () => {
        return (
            <Row justify="space-between" style={{ width: '100%', paddingTop: 20, paddingBottom: 20 }}>
                <Space direction="vertical" align="start">
                    <Button type="text" style={{ padding: 0 }} onClick={onView}>
                        <Typography.Title level={4} style={{ color: titleColor }}>
                            {policy.name}
                        </Typography.Title>
                    </Button>
                    <Typography.Text type="secondary">{policy.description}</Typography.Text>
                </Space>
                <Space direction="vertical" align="end">
                    <Typography.Title level={5}>State</Typography.Title>
                    <Tag style={{ margin: 0 }} color={isActive ? 'green' : 'red'}>
                        {policy.state}
                    </Tag>
                </Space>
            </Row>
        );
    };
    return <List.Item>{policyPreview()}</List.Item>;
}
