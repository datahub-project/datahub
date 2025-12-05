import React, { useEffect } from 'react';

import { SourceBuilderState } from '@app/ingestV2/source/builder/types';
import { IngestionSourceFormStep } from '@app/ingestV2/source/multiStepBuilder/types';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

export function ConnectionDetailsStep() {
    const { updateState, setCurrentStepCompleted, isCurrentStepCompleted } = useMultiStepContext<
        SourceBuilderState,
        IngestionSourceFormStep
    >();

    useEffect(() => {
        if (!isCurrentStepCompleted()) {
            setCurrentStepCompleted();
            updateState({
                config: {
                    recipe: '{"source":{"type":"cassandra","config":{"contact_point":"localhost","port":9042,"username":"admin","password":"password","keyspace_pattern":{"allow":[".*"]},"table_pattern":{"allow":[".*"]}}}}',
                },
                name: `test-${Date.now().toString()}`,
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return <>Connection Details Step</>;
}
