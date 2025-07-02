import { Typography } from 'antd';
import React from 'react';

import DomainSelector from '@app/domainV2/CreateNewDomainModal/DomainSelector';
import { DomainDetailsSectionProps } from '@app/domainV2/CreateNewDomainModal/types';
import { Input, TextArea } from '@src/alchemy-components';
import { Domain, EntityType } from '@src/types.generated';

const { Text } = Typography;

/**
 * Component for domain details form fields including name, description, parent domain, and advanced options
 */
const DomainDetailsSection: React.FC<DomainDetailsSectionProps> = ({
    domainName,
    setDomainName,
    domainDescription,
    setDomainDescription,
    selectedParentUrn,
    setSelectedParentUrn,
    isNestedDomainsEnabled,
}) => {
    // Convert selectedParentUrn to Domain array for the selector
    const selectedParentDomains: Domain[] = selectedParentUrn
        ? [
              {
                  urn: selectedParentUrn,
                  type: EntityType.Domain,
                  __typename: 'Domain',
              } as Domain,
          ]
        : [];

    const handleParentDomainsChange = (domains: Domain[]) => {
        // For single selection, take the first domain or empty string
        const parentUrn = domains.length > 0 ? domains[0].urn : '';
        setSelectedParentUrn(parentUrn);
    };

    return (
        <div style={{ marginBottom: '24px' }}>
            {/* Domain Name */}
            <div style={{ marginBottom: '16px' }}>
                <Input
                    label="Name *"
                    value={domainName}
                    setValue={setDomainName}
                    placeholder="A name for your domain"
                    data-testid="create-domain-name"
                />
            </div>

            {/* Domain Description */}
            <div style={{ marginBottom: '16px' }}>
                <TextArea
                    label="Description"
                    value={domainDescription}
                    onChange={(e) => setDomainDescription(e.target.value)}
                    placeholder="A description for your domain"
                    rows={4}
                    data-testid="create-domain-description"
                />
            </div>

            {/* Parent Domain (if nested domains enabled) */}
            {isNestedDomainsEnabled && (
                <div style={{ marginBottom: '16px' }}>
                    <Text strong style={{ display: 'block', marginBottom: '8px', color: '#373d44' }}>
                        Parent (optional)
                    </Text>
                    <DomainSelector
                        selectedDomains={selectedParentDomains}
                        onDomainsChange={handleParentDomainsChange}
                        placeholder="Select parent domain"
                        label=""
                        isMultiSelect={false}
                    />
                </div>
            )}
        </div>
    );
};

export default DomainDetailsSection;
