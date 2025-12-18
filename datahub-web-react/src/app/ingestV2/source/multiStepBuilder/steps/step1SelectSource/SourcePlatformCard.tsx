import { Card } from '@components';
import React from 'react';

import { SourceConfig } from '@app/ingestV2/source/builder/types';
import SourceLogo from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/SourceLogo';
import {
    CARD_HEIGHT,
    CARD_WIDTH,
    getPillLabel,
} from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/utils';

const logoStyles = {
    alignSelf: 'start',
};

interface Props {
    source: SourceConfig;
    onSelect: (platform: SourceConfig) => void;
}

export default function SourcePlatformCard({ source, onSelect }: Props) {
    return (
        <Card
            title={source.displayName}
            subTitle={source.description}
            icon={<SourceLogo sourceName={source.name} />}
            height={`${CARD_HEIGHT}px`}
            width={`${CARD_WIDTH}px`}
            noOfSubtitleLines={2}
            iconAlignment="horizontal"
            iconStyles={logoStyles}
            pillLabel={getPillLabel(source)}
            onClick={() => onSelect(source)}
            isCardClickable
        />
    );
}
