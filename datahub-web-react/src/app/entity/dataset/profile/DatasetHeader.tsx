import { Badge, Divider, message, Popover, Space, Tag, Typography } from 'antd';
import { FetchResult } from '@apollo/client';
import React, { useState } from 'react';
import styled from 'styled-components';
import MDEditor from '@uiw/react-md-editor';
import { MarkdownPreviewProps } from '@uiw/react-markdown-preview';
import { Dataset, EditableProperties } from '../../../../types.generated';
import { UpdateDatasetMutation } from '../../../../graphql/dataset.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { AvatarsGroup } from '../../../shared/avatar';
import CompactContext from '../../../shared/CompactContext';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';
import UpdateDescriptionModal from './modal/UpdateDescriptionModal';

type DescriptionTextProps = MarkdownPreviewProps & {
    isCompact: boolean;
};

const DescriptionText = styled(({ isCompact: _, ...props }: DescriptionTextProps) => <MDEditor.Markdown {...props} />)`
    ${(props) => (props.isCompact ? 'max-width: 377px;' : '')};
    display: block;
    overflow-wrap: break-word;
    word-wrap: break-word;
`;

const AddNewDescription = styled(Tag)`
    cursor: pointer;
`;
const MessageKey = 'AddDatasetDescription';

export type Props = {
    dataset: Dataset;
    updateProperties: (
        update: EditableProperties,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>>>;
    updateDescription: (
        description: string,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>>>;
};

export default function DatasetHeader({
    dataset: { description, ownership, deprecation, platform },
    // updateProperties,
    updateDescription,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const isCompact = React.useContext(CompactContext);
    const platformName = capitalizeFirstLetter(platform.name);
    const [showAddDescModal, setShowAddDescModal] = useState(false);

    const onAddDescription = async (desc: string) => {
        message.loading({ content: 'Updating...', key: MessageKey });
        await updateDescription(desc);
        message.success({ content: 'Updated!', key: MessageKey, duration: 2 });
        setShowAddDescModal(false);
    };

    return (
        <>
            <Space direction="vertical" size="middle">
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text>Dataset</Typography.Text>
                    <Typography.Text strong>{platformName}</Typography.Text>
                </Space>
                {description ? (
                    <DescriptionText isCompact={isCompact} source={description} />
                ) : (
                    <>
                        <AddNewDescription color="success" onClick={() => setShowAddDescModal(true)}>
                            + Add Description
                        </AddNewDescription>
                        {showAddDescModal && (
                            <UpdateDescriptionModal
                                title="Add description"
                                onClose={() => setShowAddDescModal(false)}
                                onSubmit={onAddDescription}
                                isAddDesc
                            />
                        )}
                    </>
                )}
                <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} size="large" />
                <div>
                    {deprecation?.deprecated && (
                        <Popover
                            placement="bottomLeft"
                            content={
                                <>
                                    <Typography.Paragraph>By: {deprecation?.actor}</Typography.Paragraph>
                                    {deprecation.decommissionTime && (
                                        <Typography.Paragraph>
                                            On: {new Date(deprecation?.decommissionTime).toUTCString()}
                                        </Typography.Paragraph>
                                    )}
                                    {deprecation?.note && (
                                        <Typography.Paragraph>{deprecation.note}</Typography.Paragraph>
                                    )}
                                </>
                            }
                            title="Deprecated"
                        >
                            <Badge count="Deprecated" />
                        </Popover>
                    )}
                </div>
            </Space>
        </>
    );
}
