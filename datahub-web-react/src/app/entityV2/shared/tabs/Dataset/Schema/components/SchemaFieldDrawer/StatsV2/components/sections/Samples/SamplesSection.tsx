/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import AllSamplesDrawer from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/components/AllSamplesDrawer';
import SampleDrawer from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/components/SampleDrawer';
import SamplesTable from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/components/SamplesTable';
import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';
import { Button } from '@src/alchemy-components';

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

    const fieldType = properties?.expandedField?.type;
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
                        icon={{ icon: 'ArrowForward' }}
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
