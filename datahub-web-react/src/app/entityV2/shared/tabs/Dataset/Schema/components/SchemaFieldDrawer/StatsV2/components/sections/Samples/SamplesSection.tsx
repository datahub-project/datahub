import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import { Button } from '@src/alchemy-components';
import useStatsTabContext from '../../../hooks/useStatsTabContext';
import SamplesTable from './components/SamplesTable';
import SampleDrawer from './components/SampleDrawer';
import AllSamplesDrawer from './components/AllSamplesDrawer';

const Container = styled.div``;

const ButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

const DEFAULT_MAX_SAMPLES_FOR_PREVIEW_TABLE = 3;

export default function SamplesSection() {
    const { properties } = useStatsTabContext();
    const [showAllSamples, setShowAllSamples] = useState<boolean>(false);
    const [selectedSample, setSelectedSample] = useState<string | null>(null);

    const fieldType = properties?.expandedField.type;
    const samples = useMemo(() => properties?.fieldProfile?.sampleValues || [], [properties]);
    const numberOfHiddenSampleValues = useMemo(() => samples.length - DEFAULT_MAX_SAMPLES_FOR_PREVIEW_TABLE, [samples]);

    // don't show the section when there are no any samples to show
    if (samples.length === 0) return null;

    return (
        <Container>
            <SamplesTable
                samples={samples}
                fieldType={fieldType}
                onViewSample={(sample) => setSelectedSample(sample)}
                maxItems={DEFAULT_MAX_SAMPLES_FOR_PREVIEW_TABLE}
            />

            {numberOfHiddenSampleValues > 0 && (
                <ButtonContainer>
                    <Button
                        variant="text"
                        onClick={() => setShowAllSamples(true)}
                        icon="ArrowForward"
                        iconPosition="right"
                    >
                        View {numberOfHiddenSampleValues} more
                    </Button>
                </ButtonContainer>
            )}
            <AllSamplesDrawer
                samples={samples}
                onBack={() => setShowAllSamples(false)}
                onViewSample={(sample) => setSelectedSample(sample)}
                open={showAllSamples}
            />
            <SampleDrawer
                sample={selectedSample || ''}
                onBack={() => setSelectedSample(null)}
                open={selectedSample !== null}
            />
        </Container>
    );
}
