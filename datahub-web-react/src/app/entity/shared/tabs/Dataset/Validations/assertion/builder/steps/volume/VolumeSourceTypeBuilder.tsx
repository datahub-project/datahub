import React from 'react';
import { Select, Typography } from 'antd';
import styled from 'styled-components';
import { InfoCircleOutlined } from '@ant-design/icons';
import { DatasetVolumeSourceType } from '../../../../../../../../../../types.generated';
import { PLATFORM_ASSERTION_CONFIGS, VOLUME_SOURCE_TYPES } from './utils';
import { ANTD_GRAY } from '../../../../../../../constants';

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
    platformUrn: string;
    value: DatasetVolumeSourceType;
    onChange: (newParams: DatasetVolumeSourceType) => void;
};

export const VolumeSourceTypeBuilder = ({ platformUrn, value, onChange }: Props) => {
    const platformConfig = PLATFORM_ASSERTION_CONFIGS[platformUrn];
    const platformDetails = platformConfig.sourceTypeDetails[value];

    return (
        <Section>
            <Typography.Title level={5}>Volume Source</Typography.Title>
            <Typography.Paragraph>
                Select the mechanism used to determine the table&apos;s row count
            </Typography.Paragraph>
            <StyledSelect value={value} onChange={(newValue) => onChange(newValue as DatasetVolumeSourceType)}>
                {platformConfig.sourceTypes.map((sourceType) => {
                    const { label, description } = VOLUME_SOURCE_TYPES[sourceType];

                    return (
                        <Select.Option key={sourceType} value={sourceType}>
                            <Typography.Text>{label}</Typography.Text>
                            <OptionDescription type="secondary">{description}</OptionDescription>
                        </Select.Option>
                    );
                })}
            </StyledSelect>
            <SourceDescription>
                <StyledInfoCircleOutlined />
                <PlatformDescription>{platformDetails.description}</PlatformDescription>
            </SourceDescription>
        </Section>
    );
};
