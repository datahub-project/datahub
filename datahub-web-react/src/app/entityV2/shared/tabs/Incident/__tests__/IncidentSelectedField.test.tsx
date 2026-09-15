import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import { Form } from 'antd';
import React from 'react';
import { afterAll, beforeAll, vi } from 'vitest';

import { IncidentSelectField } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentSelectedField';
import { INCIDENT_OPTION_LABEL_MAPPING } from '@app/entityV2/shared/tabs/Incident/constant';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

// Locks the observable behavior of IncidentSelectField's per-field placeholder and
// required-marker logic, which is keyed off the stable `key` of each field
// (category / priority / stage / state). The rendered placeholder strings
// ('Select category' / 'Select priority level' / 'Select stage') and the fact that
// only the category field is marked required form the contract this test guards.
//
// The inner SimpleSelect gates its trigger (and the placeholder) behind an
// IntersectionObserver-driven visibility flag. The global test stub reports the
// element as NOT intersecting, so we install a local stub that immediately reports
// it as intersecting to make the placeholder text reachable in the rendered DOM.
const originalIntersectionObserver = global.IntersectionObserver;

beforeAll(() => {
    class MockIntersectionObserver implements IntersectionObserver {
        readonly root = null;

        readonly rootMargin = '';

        readonly thresholds = [];

        constructor(private readonly callback: IntersectionObserverCallback) {}

        observe(target: Element) {
            this.callback(
                [{ isIntersecting: true, target } as IntersectionObserverEntry],
                this as unknown as IntersectionObserver,
            );
        }

        unobserve() {}

        disconnect() {}

        takeRecords(): IntersectionObserverEntry[] {
            return [];
        }
    }
    global.IntersectionObserver = MockIntersectionObserver as unknown as typeof IntersectionObserver;
});

afterAll(() => {
    global.IntersectionObserver = originalIntersectionObserver;
    vi.restoreAllMocks();
});

function renderField(incidentLabelMap: typeof INCIDENT_OPTION_LABEL_MAPPING.category) {
    const Wrapper = () => {
        const [form] = Form.useForm();
        return (
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <Form form={form}>
                        <IncidentSelectField
                            incidentLabelMap={incidentLabelMap}
                            options={[]}
                            form={form}
                            handleValuesChange={() => {}}
                        />
                    </Form>
                </TestPageContainer>
            </MockedProvider>
        );
    };
    return render(<Wrapper />);
}

describe('IncidentSelectField - placeholder / required behavior', () => {
    test('category field renders the "Select category" placeholder and is marked required', () => {
        const { container } = renderField(INCIDENT_OPTION_LABEL_MAPPING.category);

        expect(screen.getByText('Select category')).toBeInTheDocument();

        // antd marks a required Form.Item by putting the ant-form-item-required class on
        // the <label> inside its .ant-form-item-label cell.
        expect(container.querySelector('.ant-form-item-label label.ant-form-item-required')).toBeInTheDocument();
    });

    test('priority field renders its "Priority" label, "Select priority level" placeholder, and is NOT required', () => {
        const { container } = renderField(INCIDENT_OPTION_LABEL_MAPPING.priority);

        expect(screen.getByText('Priority')).toBeInTheDocument();
        expect(screen.getByText('Select priority level')).toBeInTheDocument();
        expect(container.querySelector('.ant-form-item-label label.ant-form-item-required')).not.toBeInTheDocument();
    });

    test('stage field renders its "Stage" label, "Select stage" placeholder, and is NOT required', () => {
        const { container } = renderField(INCIDENT_OPTION_LABEL_MAPPING.stage);

        expect(screen.getByText('Stage')).toBeInTheDocument();
        expect(screen.getByText('Select stage')).toBeInTheDocument();
        expect(container.querySelector('.ant-form-item-label label.ant-form-item-required')).not.toBeInTheDocument();
    });
});
