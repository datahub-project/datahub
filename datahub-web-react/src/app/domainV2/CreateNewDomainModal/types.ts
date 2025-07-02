import { Dispatch, SetStateAction } from 'react';

export type CreateNewDomainModalProps = {
    open: boolean;
    onClose: () => void;
    onCreate?: (
        urn: string,
        id: string | undefined,
        name: string,
        description: string | undefined,
        parentDomain?: string,
    ) => void;
};

export type DomainDetailsSectionProps = {
    domainName: string;
    setDomainName: Dispatch<SetStateAction<string>>;
    domainDescription: string;
    setDomainDescription: Dispatch<SetStateAction<string>>;
    domainId: string;
    setDomainId: Dispatch<SetStateAction<string>>;
    selectedParentUrn: string;
    setSelectedParentUrn: Dispatch<SetStateAction<string>>;
    isNestedDomainsEnabled: boolean;
};
