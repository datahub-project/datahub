import { Card, Pill } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { SourceConfig } from '@app/ingestV2/source/builder/types';
import SourceLogo from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/SourceLogo';
import {
    CARD_HEIGHT,
    CARD_WIDTH,
    PillLabel,
    getPillLabel,
} from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/utils';

const PILL_LABEL_KEYS: Record<PillLabel, string> = {
    [PillLabel.New]: 'multiStep.selectSource.pillNew',
    [PillLabel.Popular]: 'multiStep.selectSource.pillPopular',
    [PillLabel.External]: 'multiStep.selectSource.pillExternal',
};

const logoStyles = {
    alignSelf: 'start',
};

interface Props {
    source: SourceConfig;
    onSelect: (platform: SourceConfig) => void;
}

export default function SourcePlatformCard({ source, onSelect }: Props) {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const pillLabel = getPillLabel(source);
    return (
        <Card
            dataTestId={`source-option-${source.name}`}
            title={source.displayName}
            subTitle={source.description}
            icon={<SourceLogo sourceName={source.name} />}
            height={`${CARD_HEIGHT}px`}
            width={`${CARD_WIDTH}px`}
            noOfSubtitleLines={2}
            iconAlignment="horizontal"
            iconStyles={logoStyles}
            pill={
                pillLabel && (
                    <Pill
                        label={t(PILL_LABEL_KEYS[pillLabel])}
                        size="sm"
                        color={pillLabel === PillLabel.New ? 'blue' : 'primary'}
                        clickable={false}
                        variant={pillLabel === PillLabel.External ? 'outline' : 'filled'}
                    />
                )
            }
            onClick={() => onSelect(source)}
            isCardClickable
        />
    );
}
