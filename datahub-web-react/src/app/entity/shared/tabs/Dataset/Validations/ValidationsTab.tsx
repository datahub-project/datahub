import React from 'react';
import { Button } from 'antd';
import { FileDoneOutlined, FileProtectOutlined } from '@ant-design/icons';
import { useEntityData } from '../../../EntityContext';
import { TestResults } from './TestResults';
import { Assertions } from './Assertions';
import { RoutedTabs } from '../../../../../shared/RoutedTabs';

enum ViewType {
    ASSERTIONS = 'ASSERTIONS',
    TESTS = 'TESTS',
}

/**
 * Component used for rendering the Entity Validations Tab.
 */
export const ValidationsTab = () => {
    const { entityData } = useEntityData();

    const totalAssertions = (entityData as any)?.assertions?.total;
    const passingTests = (entityData as any)?.testResults?.passing || [];
    const maybeFailingTests = (entityData as any)?.testResults?.failing || [];
    const totalTests = maybeFailingTests.length + passingTests.length;

    const defaultTabPath = totalTests > 0 && totalAssertions === 0 ? ViewType.TESTS : ViewType.ASSERTIONS;

    const tabs = [
        {
            name: (
                <Button type="text" disabled={totalAssertions === 0}>
                    <FileProtectOutlined />
                    Assertions ({totalAssertions})
                </Button>
            ),
            path: ViewType.ASSERTIONS.toLocaleLowerCase(),
            content: <Assertions />,
            display: {
                enabled: () => true,
            },
        },
        {
            name: (
                <Button type="text" disabled={totalTests === 0}>
                    <FileDoneOutlined />
                    Tests ({totalTests})
                </Button>
            ),
            path: ViewType.TESTS.toLocaleLowerCase(),
            content: <TestResults passing={passingTests} failing={maybeFailingTests} />,
            display: {
                enabled: () => true,
            },
        },
    ];

    return (
        <>
            <RoutedTabs defaultPath={defaultTabPath} tabs={tabs} />
        </>
    );
};
