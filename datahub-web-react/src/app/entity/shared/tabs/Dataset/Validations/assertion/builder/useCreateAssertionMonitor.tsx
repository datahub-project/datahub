import { message } from 'antd';
import analytics, { EventType } from '../../../../../../../analytics';
import { useCreateFreshnessAssertionMutation } from '../../../../../../../../graphql/assertion.generated';
import { AssertionType, Monitor } from '../../../../../../../../types.generated';
import {
    builderStateToCreateAssertionMonitorVariables,
    builderStateToCreateFreshnessAssertionVariables,
} from './utils';
import { useCreateAssertionMonitorMutation } from '../../../../../../../../graphql/monitor.generated';

export const useCreateAssertionMonitor = (builderState, onCreate): (() => void) => {
    /**
     * Mutations for creating Assertions, and the Monitor that evaluates them.
     */
    const [createFreshnessAssertionMutation] = useCreateFreshnessAssertionMutation();
    const [createAssertionMonitorMutation] = useCreateAssertionMonitorMutation();

    /**
     * Get the mutation to use for the Assertion type being created.
     */
    const getCreateAssertionMutation = () => {
        switch (builderState.assertion?.type) {
            case AssertionType.Freshness:
                return createFreshnessAssertionMutation;
            default:
                return null;
        }
    };

    /**
     * Get the mutation variables to use for the Assertion type being created.
     */
    const getCreateAssertionVariables = () => {
        switch (builderState.assertion?.type) {
            case AssertionType.Freshness:
                return builderStateToCreateFreshnessAssertionVariables(builderState);
            default:
                return null;
        }
    };

    /**
     * Create a monitor to evaluate the new assertions
     */
    const createMonitor = (assertionUrn) => {
        const variables = builderStateToCreateAssertionMonitorVariables(assertionUrn, builderState);
        createAssertionMonitorMutation({
            variables: variables as any,
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.CreateAssertionMonitorEvent,
                        assertionType: builderState.assertion?.type as string,
                    });
                    message.success({
                        content: `Created new Assertion Monitor!`,
                        duration: 3,
                    });
                    // TODO - Generalize.
                    onCreate?.(data?.createAssertionMonitor as Monitor);
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to create Assertion Monitor! An unexpected error occurred' });
            });
    };

    /**
     * Create a new AssertionMonitor :)
     *
     * Notice that this creates 2 things:
     *
     * 1. A native assertion associated with the entity.
     * 2. A monitor which is responsible for evaluating this assertion.
     */
    const createAssertionMonitor = () => {
        const createAssertionMutation = getCreateAssertionMutation();
        const createAssertionVariables = getCreateAssertionVariables();

        if (createAssertionMutation && createAssertionVariables) {
            createAssertionMutation({
                variables: createAssertionVariables as any,
            })
                .then(({ data, errors }) => {
                    if (!errors) {
                        createMonitor(data?.createFreshnessAssertion?.urn);
                    }
                })
                .catch(() => {
                    message.destroy();
                    message.error({ content: 'Failed to create Assertion Monitor! An unexpected error occurred' });
                });
        }
    };

    return createAssertionMonitor;
};
