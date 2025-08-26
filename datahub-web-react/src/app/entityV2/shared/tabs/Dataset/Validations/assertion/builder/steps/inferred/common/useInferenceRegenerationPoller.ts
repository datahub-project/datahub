import { useEffect, useRef } from 'react';

import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';

import { GetAssertionWithMonitorsQuery } from '@graphql/monitor.generated';
import { Assertion, EmbeddedAssertion, Monitor } from '@types';

const REGENERATION_TIMEOUT_MS = 1 * 60 * 1000; // 1 minutes
const POLLING_INTERVAL_MS = 3000; // Poll every 3 seconds

/**
 * Extracts predictions from embedded volume assertions
 * @param embeddedAssertions - The new embedded assertions to extract predictions from
 * @returns An array of predictions with index, lowerBound, upperBound, and timeWindow
 */
export const extractPredictionsFromEmbeddedVolumeAssertions = (embeddedAssertions: EmbeddedAssertion[]) => {
    if (!embeddedAssertions || embeddedAssertions.length === 0) return [];
    return embeddedAssertions.map((embeddedAssertion, index) => {
        const lowerBound = embeddedAssertion.assertion?.volumeAssertion?.rowCountTotal?.parameters?.minValue?.value;
        const upperBound = embeddedAssertion.assertion?.volumeAssertion?.rowCountTotal?.parameters?.maxValue?.value;
        const timeWindow = embeddedAssertion.evaluationTimeWindow;
        return {
            index,
            lowerBound: lowerBound ? Number(lowerBound) : undefined,
            upperBound: upperBound ? Number(upperBound) : undefined,
            timeWindow: timeWindow
                ? {
                      startTimeMillis: timeWindow.startTimeMillis,
                      endTimeMillis: timeWindow.endTimeMillis,
                  }
                : undefined,
        };
    });
};

/**
 * Extracts embedded assertions and generated at timestamp from assertion data
 * @param data - The data object to extract assertion data from
 * @returns An object containing the embedded assertions and the generated at timestamp
 */
export const extractAssertionData = (data: GetAssertionWithMonitorsQuery) => {
    const assertion = data?.assertion as Assertion;
    const monitor = assertion?.monitor?.relationships?.[0]?.entity as Monitor;
    const assertions = monitor?.info?.assertionMonitor?.assertions || [];
    const firstAssertion = assertions[0];
    const embeddedAssertions = firstAssertion?.context?.embeddedAssertions || [];
    const generatedAt = firstAssertion?.context?.inferenceDetails?.generatedAt ?? Date.now();

    return { embeddedAssertions, generatedAt };
};

/**
 * Props for the useInferenceRegenerationPoller hook
 * @param isRegenerating - Whether the regeneration is currently in progress
 * @param generatedAt - The timestamp of the current generation
 * @param refetch - The function to refetch the data
 * @param previousGeneratedAt - The timestamp of the previous generation
 * @param setPreviousPredictions - The function to set the previous predictions
 * @param setIsRegenerating - The function to set the regeneration state
 * @param setHasTimedOut - The function to set the timeout state
 * @param setInitialSettings - The function to set the initial settings
 * @param state - The current state of the assertion monitor builder
 */
interface UseInferenceRegenerationPollerProps {
    isRegenerating: boolean;
    generatedAt: number | undefined;
    refetch: () => Promise<any>;
    previousGeneratedAt: React.MutableRefObject<number | undefined>;
    setPreviousPredictions: React.Dispatch<
        React.SetStateAction<
            Array<{
                index: number;
                lowerBound?: number;
                upperBound?: number;
                timeWindow?: { startTimeMillis?: number; endTimeMillis?: number };
            }>
        >
    >;
    setIsRegenerating: React.Dispatch<React.SetStateAction<boolean>>;
    setHasTimedOut: React.Dispatch<React.SetStateAction<boolean>>;
    setInitialSettings: React.Dispatch<React.SetStateAction<any>>;
    state: AssertionMonitorBuilderState;
}

export const useInferenceRegenerationPoller = ({
    isRegenerating,
    generatedAt,
    refetch,
    previousGeneratedAt,
    setPreviousPredictions,
    setIsRegenerating,
    setHasTimedOut,
    setInitialSettings,
    state,
}: UseInferenceRegenerationPollerProps) => {
    const regenerationTimeout = useRef<NodeJS.Timeout>();
    const pollingInterval = useRef<NodeJS.Timeout>();

    useEffect(() => {
        const mounted = { value: true };

        const cleanup = () => {
            mounted.value = false;
            if (pollingInterval.current) {
                clearInterval(pollingInterval.current);
                pollingInterval.current = undefined;
            }
            if (regenerationTimeout.current) {
                clearTimeout(regenerationTimeout.current);
                regenerationTimeout.current = undefined;
            }
        };

        if (isRegenerating && generatedAt) {
            // Start polling immediately
            const pollForUpdates = async () => {
                try {
                    const result = await refetch();
                    if (!mounted.value) return;

                    const { embeddedAssertions: newEmbeddedAssertions, generatedAt: currentGeneratedAt } =
                        extractAssertionData(result.data);

                    const hasData = newEmbeddedAssertions.length > 0;
                    const isDataNew =
                        hasData &&
                        (previousGeneratedAt.current === undefined || currentGeneratedAt > previousGeneratedAt.current);

                    if (isDataNew) {
                        if (mounted.value) {
                            const newPredictions =
                                extractPredictionsFromEmbeddedVolumeAssertions(newEmbeddedAssertions);
                            setPreviousPredictions(newPredictions);
                            setIsRegenerating(false);
                            setHasTimedOut(false);
                            setInitialSettings(state.inferenceSettings);
                        }
                        cleanup();
                    }
                } catch (error) {
                    console.error('Error polling for updates:', error);
                    if (mounted.value) {
                        setIsRegenerating(false);
                    }
                    cleanup();
                }
            };

            // Poll immediately and then set up interval
            pollForUpdates();
            pollingInterval.current = setInterval(pollForUpdates, POLLING_INTERVAL_MS);

            regenerationTimeout.current = setTimeout(() => {
                if (mounted.value) {
                    setHasTimedOut(true);
                    setIsRegenerating(false);
                }
                cleanup();
            }, REGENERATION_TIMEOUT_MS);
        }

        return cleanup;
    }, [
        isRegenerating,
        generatedAt,
        refetch,
        state.inferenceSettings,
        previousGeneratedAt,
        setHasTimedOut,
        setInitialSettings,
        setIsRegenerating,
        setPreviousPredictions,
    ]);
};
