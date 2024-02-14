import React from 'react';
import { Select, Typography } from 'antd';
import styled from 'styled-components';
import { InfoCircleOutlined } from '@ant-design/icons';
import { DatasetVolumeSourceType } from '../../../../../../../../../../types.generated';
import { VOLUME_SOURCE_TYPES, getVolumeSourceTypeDetails, getVolumeSourceTypeOptions } from './utils';
import { ANTD_GRAY } from '../../../../../../../constants';
import { useConnectionForEntityExists } from '../../../../acrylUtils';

const StyledSelect = styled(Select)`
    width: 300px;
`;

const Section = styled.div`
    margin-bottom: 16px;
`;

const OptionDescription = styled(Typography.Paragraph)`
    && {
        word-wrap: break-word;
        white-space: break-spaces;
    }
`;

const SourceDescription = styled.div`
    margin-top: 12px;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    justify-content: left;
    border: 1px solid ${ANTD_GRAY[5]};
    padding: 12px;
    background-color: ${ANTD_GRAY[3]};
    border-radius: 8px;
    margin-bottom: 20px;
`;

const PlatformDescription = styled.div`
    margin-left: 8px;
`;

const StyledInfoCircleOutlined = styled(InfoCircleOutlined)`
    color: ${ANTD_GRAY[7]};
    margin-right: 4px;
`;

type Props = {
    entityUrn: string;
    platformUrn: string;
    value: DatasetVolumeSourceType;
    onChange: (newParams: DatasetVolumeSourceType) => void;
    disabled?: boolean;
};

export const VolumeSourceTypeBuilder = ({ entityUrn, platformUrn, value, onChange, disabled }: Props) => {
    const connectionForEntityExists = useConnectionForEntityExists(entityUrn);
    const sourceOptions = getVolumeSourceTypeOptions(platformUrn, connectionForEntityExists);
    const sourceDetails = getVolumeSourceTypeDetails(platformUrn, value);

    return (
        <Section>
            <Typography.Title level={5}>Volume Source</Typography.Title>
            <Typography.Paragraph>
                Select the mechanism used to determine the table&apos;s row count
            </Typography.Paragraph>
            <StyledSelect
                value={value}
                onChange={(newValue) => onChange(newValue as DatasetVolumeSourceType)}
                disabled={disabled}
            >
                {sourceOptions.map((sourceType) => {
                    const { label, description } = VOLUME_SOURCE_TYPES[sourceType];

                    return (
                        <Select.Option key={sourceType} value={sourceType}>
                            <Typography.Text>{label}</Typography.Text>
                            <OptionDescription type="secondary">{description}</OptionDescription>
                        </Select.Option>
                    );
                })}
            </StyledSelect>
            {sourceDetails && (
                <SourceDescription>
                    <StyledInfoCircleOutlined />
                    <PlatformDescription>{sourceDetails.description}</PlatformDescription>
                </SourceDescription>
            )}
        </Section>
    );
};
