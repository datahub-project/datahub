import { Icon, Text, Tooltip } from '@components';
import { Binary } from '@phosphor-icons/react/dist/csr/Binary';
import { ChartLine } from '@phosphor-icons/react/dist/csr/ChartLine';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Hash } from '@phosphor-icons/react/dist/csr/Hash';
import { Image } from '@phosphor-icons/react/dist/csr/Image';
import { List } from '@phosphor-icons/react/dist/csr/List';
import { ListNumbers } from '@phosphor-icons/react/dist/csr/ListNumbers';
import { Microphone } from '@phosphor-icons/react/dist/csr/Microphone';
import { Prohibit } from '@phosphor-icons/react/dist/csr/Prohibit';
import { Question } from '@phosphor-icons/react/dist/csr/Question';
import { TextT } from '@phosphor-icons/react/dist/csr/TextT';
import { VideoCamera } from '@phosphor-icons/react/dist/csr/VideoCamera';
import React from 'react';
import styled from 'styled-components';

import { FontSizeOptions } from '@components/theme/config';

import { capitalizeFirstLetter } from '@app/shared/textUtil';

import { MlFeatureDataType } from '@types';

const TypeIconContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    text-align: center;
    margin-top: 2.5px;
    width: 70px;
`;

const TypeSubtitle = styled(Text)<{ hasicon?: string }>`
    font-size: 8px;
    text-align: center;
    text-transform: uppercase;
    ${(props) => (props.hasicon ? '' : 'margin-top: 4px;')}
`;

type PhosphorIcon = React.ComponentType<any>;

/* untranslated-text -- ML feature data-type enum display labels, mirror GraphQL MlFeatureDataType enum */
const DATA_TYPE_ICON_MAP: Record<
    MlFeatureDataType,
    { icon: PhosphorIcon | null; size: FontSizeOptions; text: string }
> = {
    [MlFeatureDataType.Byte]: { icon: Binary, size: 'xl', text: 'Byte' },
    [MlFeatureDataType.Time]: { icon: Clock, size: 'xl', text: 'Time' },
    [MlFeatureDataType.Set]: { icon: List, size: 'xl', text: 'Set' },
    [MlFeatureDataType.Unknown]: { icon: Question, size: 'lg', text: 'Unknown' },
    [MlFeatureDataType.Map]: { icon: List, size: 'md', text: 'Map' },
    [MlFeatureDataType.Useless]: { icon: Prohibit, size: 'xl', text: 'Useless' },
    [MlFeatureDataType.Nominal]: { icon: Hash, size: 'md', text: 'Nominal' },
    [MlFeatureDataType.Ordinal]: { icon: ListNumbers, size: 'xl', text: 'Ordinal' },
    [MlFeatureDataType.Binary]: { icon: Binary, size: 'lg', text: 'Binary' },
    [MlFeatureDataType.Count]: { icon: Hash, size: 'md', text: 'Count' },
    [MlFeatureDataType.Interval]: { icon: Clock, size: 'lg', text: 'Interval' },
    [MlFeatureDataType.Image]: { icon: Image, size: 'lg', text: 'Image' },
    [MlFeatureDataType.Video]: { icon: VideoCamera, size: 'lg', text: 'Video' },
    [MlFeatureDataType.Audio]: { icon: Microphone, size: 'lg', text: 'Audio' },
    [MlFeatureDataType.Text]: { icon: TextT, size: 'xl', text: 'Text' },
    [MlFeatureDataType.Sequence]: { icon: ListNumbers, size: 'lg', text: 'Sequence' },
    [MlFeatureDataType.Continuous]: { icon: ChartLine, size: 'lg', text: 'Continuous' },
};

type Props = {
    dataType?: MlFeatureDataType;
};

export default function MlFeatureDataTypeIcon({ dataType }: Props) {
    const { icon: IconComponent, size, text } = DATA_TYPE_ICON_MAP[dataType || MlFeatureDataType.Unknown];

    // eslint-disable-next-line react/prop-types
    const NativeDataTypeTooltip = ({ children }) => (
        <Tooltip placement="top" title={capitalizeFirstLetter(text)}>
            {children}
        </Tooltip>
    );

    return (
        <NativeDataTypeTooltip>
            <TypeIconContainer>
                {IconComponent && <Icon icon={IconComponent} size={size} />}
                <TypeSubtitle
                    type="span"
                    color="textSecondary"
                    /* eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) programmatic flag value for styled-component prop, not user-facing copy */
                    hasicon={IconComponent ? 'yes' : undefined}
                >
                    {text}
                </TypeSubtitle>
            </TypeIconContainer>
        </NativeDataTypeTooltip>
    );
}
