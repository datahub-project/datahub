import {
    UnorderedListOutlined,
    FieldStringOutlined,
    ClockCircleOutlined,
    QuestionOutlined,
    StopOutlined,
    OrderedListOutlined,
    NumberOutlined,
    AudioOutlined,
    VideoCameraOutlined,
    FileImageOutlined,
    FieldBinaryOutlined,
    LineChartOutlined,
} from '@ant-design/icons';
import { Tooltip, Typography } from 'antd';
import React, { FC } from 'react';
import { VscFileBinary } from 'react-icons/vsc';
import styled from 'styled-components';
import { capitalizeFirstLetter } from '../../../../shared/textUtil';
import { MlFeatureDataType } from '../../../../../types.generated';

const TypeIconContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    text-align: center;
    margin-top: 2.5px;
    width: 70px;
`;

const TypeSubtitle = styled(Typography.Text)<{ hasicon?: string }>`
    font-size: 8px;
    text-align: center;
    text-transform: uppercase;
    ${(props) => (props.hasicon ? '' : 'margin-top: 4px;')}
`;

const IconSpan = styled.span`
    font-size: 18px;
`;

const DATA_TYPE_ICON_MAP: Record<MlFeatureDataType, { icon: FC<{ style: any }> | null; size: number; text: string }> = {
    [MlFeatureDataType.Byte]: {
        icon: () => (
            <IconSpan role="img" aria-label="calendar" className="anticon anticon-calendar">
                <VscFileBinary />
            </IconSpan>
        ),
        size: 18,
        text: 'Byte',
    },
    [MlFeatureDataType.Time]: { icon: ClockCircleOutlined, size: 18, text: 'Time' },
    [MlFeatureDataType.Set]: { icon: UnorderedListOutlined, size: 18, text: 'Set' },
    [MlFeatureDataType.Unknown]: { icon: QuestionOutlined, size: 16, text: 'Unknown' },
    [MlFeatureDataType.Map]: { icon: UnorderedListOutlined, size: 14, text: 'Map' },
    [MlFeatureDataType.Useless]: { icon: StopOutlined, size: 18, text: 'Useless' },
    [MlFeatureDataType.Nominal]: { icon: NumberOutlined, size: 14, text: 'Nominal' },
    [MlFeatureDataType.Ordinal]: { icon: OrderedListOutlined, size: 18, text: 'Ordinal' },
    [MlFeatureDataType.Binary]: { icon: FieldBinaryOutlined, size: 16, text: 'Binary' },
    [MlFeatureDataType.Count]: { icon: NumberOutlined, size: 14, text: 'Count' },
    [MlFeatureDataType.Interval]: { icon: ClockCircleOutlined, size: 16, text: 'Interval' },
    [MlFeatureDataType.Image]: { icon: FileImageOutlined, size: 16, text: 'Image' },
    [MlFeatureDataType.Video]: { icon: VideoCameraOutlined, size: 16, text: 'Video' },
    [MlFeatureDataType.Audio]: { icon: AudioOutlined, size: 16, text: 'Audio' },
    [MlFeatureDataType.Text]: { icon: FieldStringOutlined, size: 18, text: 'Text' },
    [MlFeatureDataType.Sequence]: { icon: OrderedListOutlined, size: 16, text: 'Sequence' },
    [MlFeatureDataType.Continuous]: { icon: LineChartOutlined, size: 16, text: 'Continuous' },
};

type Props = {
    dataType?: MlFeatureDataType;
};

export default function MlFeatureDataTypeIcon({ dataType }: Props) {
    const { icon: Icon, size, text } = DATA_TYPE_ICON_MAP[dataType || MlFeatureDataType.Unknown];

    // eslint-disable-next-line react/prop-types
    const NativeDataTypeTooltip = ({ children }) => (
        <Tooltip placement="top" title={capitalizeFirstLetter(text)}>
            {children}
        </Tooltip>
    );

    return (
        <NativeDataTypeTooltip>
            <TypeIconContainer>
                {Icon && <Icon style={{ fontSize: size }} />}
                <TypeSubtitle type="secondary" hasicon={Icon ? 'yes' : undefined}>
                    {text}
                </TypeSubtitle>
            </TypeIconContainer>
        </NativeDataTypeTooltip>
    );
}
