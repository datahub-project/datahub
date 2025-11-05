export enum SourceCapability {
    }

interface ConnectionCapability {
    capable: boolean;
    failure_reason: string | null;
    mitigation_message: string | null;
}

export interface CapabilityReport {
    [key: string]: ConnectionCapability;
}

export interface TestConnectionResult {
    internal_failure?: boolean;
    internal_failure_reason?: string;
    basic_connectivity?: ConnectionCapability;
    capability_report?: CapabilityReport;
}
