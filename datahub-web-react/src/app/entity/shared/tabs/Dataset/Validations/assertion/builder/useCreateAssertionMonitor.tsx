import { message } from 'antd';
import analytics, { EventType } from '../../../../../../../analytics';
import {
    useCreateFieldAssertionMutation,
    useCreateFreshnessAssertionMutation,
    useCreateSqlAssertionMutation,
    useCreateVolumeAssertionMutation,
} from '../../../../../../../../graphql/assertion.generated';
import { AssertionType, Monitor, Assertion } from '../../../../../../../../types.generated';
import {
    builderStateToCreateAssertionMonitorVariables,
    builderStateToCreateFieldAssertionVariables,
    builderStateToCreateFreshnessAssertionVariables,
    builderStateToCreateSqlAssertionVariables,
    builderStateToCreateVolumeAssertionVariables,
} from './utils';
import { useCreateAssertionMonitorMutation } from '../../../../../../../../graphql/monitor.generated';

export const useCreateAssertionMonitor = (entityUrn, builderState, onCreate): (() => Promise<void>) => {
    /**
     * Mutations for creating Assertions, and the Monitor that evaluates them.
     */
    const [createFreshnessAssertionMutation] = useCreateFreshnessAssertionMutation();
    const [createVolumeAssertionMutation] = useCreateVolumeAssertionMutation();
    const [createSqlAssertionMutation] = useCreateSqlAssertionMutation();
    const [createFieldAssertionMutation] = useCreateFieldAssertionMutation();
    const [createAssertionMonitorMutation] = useCreateAssertionMonitorMutation();

    /**
     * Get the mutation to use for the Assertion type being created.
     */
    const getCreateAssertionMutation = () => {
        switch (builderState.assertion?.type) {
            case AssertionType.Freshness:
                return createFreshnessAssertionMutation;
            case AssertionType.Volume:
                return createVolumeAssertionMutation;
            case AssertionType.Sql:
                return createSqlAssertionMutation;
            case AssertionType.Field:
                return createFieldAssertionMutation;
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
            case AssertionType.Volume:
                return builderStateToCreateVolumeAssertionVariables(builderState);
            case AssertionType.Sql:
                return builderStateToCreateSqlAssertionVariables(builderState);
            case AssertionType.Field:
                return builderStateToCreateFieldAssertionVariables(builderState);
            default:
                return null;
        }
    };

    /**
     * Create a monitor to evaluate the new assertions
     */
    const createMonitor = (assertion: Assertion) => {
        const variables = builderStateToCreateAssertionMonitorVariables(assertion.urn, builderState);
        createAssertionMonitorMutation({
            variables: variables as any,
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.CreateAssertionMonitorEvent,
                        assertionType: builderState.assertion?.type as string,
                        entityUrn,
                    });
                    message.success({
                        content: `Created new Assertion Monitor!`,
                        duration: 3,
                    });
                    onCreate?.(assertion, data?.createAssertionMonitor as Monitor);
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
            return createAssertionMutation({
                variables: createAssertionVariables as any,
            })
                .then(({ data, errors }: any) => {
                    if (!errors) {
                        const assertion =
                            data?.createFreshnessAssertion ||
                            data?.createVolumeAssertion ||
                            data?.createSqlAssertion ||
                            data?.createFieldAssertion;
                        return createMonitor(assertion as Assertion);
                    }
                    throw new Error('Encountered errors while creating assertion');
                })
                .catch(() => {
                    message.destroy();
                    message.error({ content: 'Failed to create Assertion Monitor! An unexpected error occurred' });
                });
        }

        message.destroy();
        message.error({ content: 'Failed to create Assertion Monitor! An unexpected error occurred' });
        return Promise.reject(new Error('Could not find createAssertionMutation or createAssertionVariables!'));
    };

    return createAssertionMonitor;
};
