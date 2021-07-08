import { UnorderedListOutlined, QuestionCircleOutlined, FieldTimeOutlined, KeyOutlined } from '@ant-design/icons';
import { Tooltip, Typography } from 'antd';
import React, { FC } from 'react';
import { VscFileBinary } from 'react-icons/vsc';
import styled from 'styled-components';
import { capitalizeFirstLetter } from '../../../../shared/capitalizeFirstLetter';
import { EntityType, MlFeatureDataType } from '../../../../../types.generated';

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

const Container = styled.div`
    display: flex;
    flex-direction: row;
`;

const DATA_TYPE_ICON_MAP: Record<MlFeatureDataType, { icon: FC<{ style: any }> | null; size: number; text: string }> = {
    [MlFeatureDataType.Byte]: {
        icon: () => (
            <IconSpan role="img" aria-label="calendar" className="anticon anticon-calendar">
                <VscFileBinary />
            </IconSpan>
        ),
        size: 18,
        text: 'Bytes',
    },
    [MlFeatureDataType.Time]: { icon: FieldTimeOutlined, size: 18, text: 'Time' },
    [MlFeatureDataType.Set]: { icon: UnorderedListOutlined, size: 18, text: 'SET' },
    [MlFeatureDataType.Unknown]: { icon: QuestionCircleOutlined, size: 16, text: 'UNKNOWN' },
    [MlFeatureDataType.Map]: { icon: UnorderedListOutlined, size: 14, text: 'Array' },
    [MlFeatureDataType.Useless]: { icon: null, size: 0, text: 'Useless' },
    [MlFeatureDataType.Nominal]: { icon: null, size: 0, text: 'Nominal' },
    [MlFeatureDataType.Ordinal]: { icon: null, size: 0, text: 'Ordinal' },
    [MlFeatureDataType.Binary]: { icon: null, size: 0, text: 'Binary' },
    [MlFeatureDataType.Count]: { icon: null, size: 0, text: 'Count' },
    [MlFeatureDataType.Interval]: { icon: null, size: 0, text: 'Interval' },
    [MlFeatureDataType.Image]: { icon: null, size: 0, text: 'Image' },
    [MlFeatureDataType.Video]: { icon: null, size: 0, text: 'Video' },
    [MlFeatureDataType.Audio]: { icon: null, size: 0, text: 'Audio' },
    [MlFeatureDataType.Text]: { icon: null, size: 0, text: 'Text' },
    [MlFeatureDataType.Sequence]: { icon: null, size: 0, text: 'Sequence' },
    [MlFeatureDataType.Continuous]: { icon: null, size: 0, text: 'Continuous' },
};

type Props = {
    dataType?: MlFeatureDataType;
    entityType?: EntityType;
};

export default function TypeIcon({ dataType, entityType }: Props) {
    const { icon: Icon, size, text } = DATA_TYPE_ICON_MAP[dataType || MlFeatureDataType.Unknown];

    // eslint-disable-next-line react/prop-types
    const NativeDataTypeTooltip = ({ children }) => (
        <Tooltip placement="top" title={capitalizeFirstLetter(text)}>
            {children}
        </Tooltip>
    );

    return (
        <Container>
            {entityType === EntityType.MlprimaryKey && <KeyOutlined />}
            <NativeDataTypeTooltip>
                <TypeIconContainer>
                    {Icon && <Icon style={{ fontSize: size }} />}
                    <TypeSubtitle type="secondary" hasicon={Icon ? 'yes' : undefined}>
                        {text}
                    </TypeSubtitle>
                </TypeIconContainer>
            </NativeDataTypeTooltip>
        </Container>
    );
}
