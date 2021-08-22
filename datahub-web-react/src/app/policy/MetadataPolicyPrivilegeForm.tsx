import React, { useState } from 'react';
import { Form, Select, Typography } from 'antd';

export default function MetadataPolicyPrivilegeForm() {
    const [assetType, setAssetType] = useState('');
    const [assetUrns, setAssetUrns] = useState([]);
    const [privileges, setPrivileges] = useState([]);

    const onSelectPrivilege = (privilege: string) => {
        const newPrivs = [...privileges, privilege];
        setPrivileges(newPrivs as never[]);
    };

    const onSelectAsset = (asset: string) => {
        const newAssets = [...assetUrns, asset as string];
        setAssetUrns(newAssets as never[]);
    };

    const assetSearchResults = [
        {
            urn: 'urn:li:dataset:1',
            name: 'My test dataset',
        },
    ];

    return (
        <Form layout="vertical" initialValues={{}} style={{ margin: 12, marginTop: 36, marginBottom: 40 }}>
            <Form.Item label={<Typography.Text strong>Asset Type</Typography.Text>} labelAlign="right">
                <Typography.Paragraph>
                    Select the specific type of asset this policy should apply to, or all if it should apply to all
                    assets.
                </Typography.Paragraph>
                <Select onSelect={(type) => setAssetType(type as string)}>
                    <Select.Option value="Dataset">Dataset (Streams, Tables, Views)</Select.Option>
                    <Select.Option value="Chart">Chart</Select.Option>
                    <Select.Option value="Dashboard">Dashboard</Select.Option>
                    <Select.Option value="DataPipeline">Data Pipeline</Select.Option>
                    <Select.Option value="DataTask">Data Task</Select.Option>
                    <Select.Option value="Tag">Tag</Select.Option>
                </Select>
            </Form.Item>
            {assetType && (
                <Form.Item label={<Typography.Text strong>Asset</Typography.Text>}>
                    <Typography.Paragraph>
                        Search for specific assets the policy should apply to, or select <b>All</b> to apply the policy
                        to all assets of the given type.
                    </Typography.Paragraph>
                    <Select
                        mode="multiple"
                        placeholder="Search for data assets..."
                        onSelect={(asset: any) => onSelectAsset(asset)}
                    >
                        {assetSearchResults.map((result) => (
                            <Select.Option value={result.urn}>{result.name}</Select.Option>
                        ))}
                    </Select>
                </Form.Item>
            )}
            {assetUrns.length > 0 && (
                <>
                    <Form.Item
                        label={<Typography.Text strong>Privileges</Typography.Text>}
                        rules={[
                            {
                                required: true,
                                message: 'Please select your permissions.',
                                type: 'array',
                            },
                        ]}
                    >
                        <Typography.Paragraph>Select a set of privileges to permit.</Typography.Paragraph>
                        <Select mode="multiple" onSelect={(value: string) => onSelectPrivilege(value)}>
                            <Select.Option value="editSelfEntityOwner">Claim Ownership</Select.Option>
                            <Select.Option value="addEntityTag">Add an Entity Tag</Select.Option>
                            <Select.Option value="addEntityTerm">Add an Entity Term</Select.Option>
                            <Select.Option value="addEntityDocLink">Add an Link</Select.Option>
                            <Select.Option value="editEntityTags">Edit Entity Tags</Select.Option>
                            <Select.Option value="editEntityTerms">Edit Entity Terms</Select.Option>
                            <Select.Option value="editEntityOwners">Edit Entity Owners</Select.Option>
                            <Select.Option value="editEntityDocs">Edit Entity Documentation</Select.Option>
                            <Select.Option value="editEntityStatus">Edit Entity Status</Select.Option>
                            <Select.Option value="editEntityDocLinks">Edit Entity Links</Select.Option>
                            <Select.Option value="editEntityColDocs">Edit Entity Column Documentation</Select.Option>
                            <Select.Option value="editEntityColTags">Edit Entity Column Tags</Select.Option>
                            <Select.Option value="any">All Privileges</Select.Option>
                        </Select>
                    </Form.Item>
                </>
            )}
        </Form>
    );
}
