import React from 'react';

export enum CompliancePropertyQualifiedName {
    Annotations = 'compliance.annotations',
    IsExempted = 'compliance.is_exempted',
    LastCheckDate = 'compliance.last.check.date',
    LastStatus = 'compliance.last.status',
    NonComplyingRules = 'compliance.non_complying_rule',
    RecordsClasses = 'compliance.record_class',
    RetentionColumn = 'compliance.retention.column',
    RetentionDays = 'compliance.retention.days',
    RetentionJira = 'compliance.retention.jira',
    ScrubbingOp = 'compliance.scrubbing.operation',
    ScrubbingState = 'compliance.scrubbing.state',
    ScrubbingStatus = 'compliance.scrubbing.status',
}
