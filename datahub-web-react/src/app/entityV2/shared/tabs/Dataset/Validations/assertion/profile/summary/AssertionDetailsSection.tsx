import { CollapsiblePanel, Icon } from '@components';
import { Code } from '@phosphor-icons/react/dist/csr/Code';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { CodeBlock } from '@components/components/CodeBlock';

import { Assertion, AssertionType } from '@types';

const SectionTitle = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
`;

type Props = {
    assertion: Assertion;
};

/**
 * Extract logic/SQL from the assertion based on its type.
 */
export function getLogicFromAssertion(assertion: Assertion): string | null {
    const info = assertion?.info;
    if (!info) return null;

    switch (info.type) {
        case AssertionType.Custom:
            return info.customAssertion?.logic || null;
        case AssertionType.Dataset:
            return info.datasetAssertion?.logic || null;
        case AssertionType.Sql:
            return info.sqlAssertion?.statement || null;
        default:
            return null;
    }
}

export function hasAssertionDetails(assertion: Assertion): boolean {
    return !!getLogicFromAssertion(assertion) || !!assertion?.info?.customProperties?.length;
}

/**
 * Collapsible section showing assertion logic (SQL) and custom properties.
 * Adapted from community contribution https://github.com/datahub-project/datahub/pull/16030.
 */
export const AssertionDetailsSection = ({ assertion }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const info = assertion?.info;
    const logic = getLogicFromAssertion(assertion);

    const customProperties = info?.customProperties;
    const hasCustomProperties = !!customProperties && customProperties.length > 0;

    const customPropertiesObject = hasCustomProperties
        ? customProperties.reduce(
              (acc, prop) => {
                  acc[prop.key] = prop.value ?? '';
                  return acc;
              },
              {} as Record<string, string>,
          )
        : null;

    if (!hasAssertionDetails(assertion)) {
        return null;
    }

    return (
        <>
            {logic && (
                <CollapsiblePanel
                    defaultOpen
                    header={
                        <SectionTitle>
                            <Icon icon={Code} size="md" />
                            <span>{t('assertionSummary.detailsLogicTitle')}</span>
                        </SectionTitle>
                    }
                >
                    <CodeBlock code={logic} wrap maxHeight={400} showFormat={false} />
                </CollapsiblePanel>
            )}
            {hasCustomProperties && (
                <CollapsiblePanel
                    header={
                        <SectionTitle>
                            <Icon icon={Info} size="md" />
                            <span>{t('assertionSummary.detailsCustomPropertiesTitle')}</span>
                        </SectionTitle>
                    }
                >
                    <CodeBlock
                        code={JSON.stringify(customPropertiesObject, null, 2)}
                        language="json"
                        wrap
                        maxHeight={300}
                        showFormat={false}
                    />
                </CollapsiblePanel>
            )}
        </>
    );
};
