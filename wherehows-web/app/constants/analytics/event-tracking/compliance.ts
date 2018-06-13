/**
 * Enumerates the available compliance metadata events
 */
enum ComplianceEvent {
  Cancel = 'CancelEditComplianceMetadata',
  Next = 'NextComplianceMetadataStep',
  ManualApply = 'AdvancedEditComplianceMetadataStep',
  Previous = 'PreviousComplianceMetadataStep',
  Edit = 'BeginEditComplianceMetadata',
  Download = 'DownloadComplianceMetadata',
  Upload = 'UploadComplianceMetadata',
  SetUnspecifiedAsNone = 'SetUnspecifiedFieldsAsNone',
  FieldIndentifier = 'ComplianceMetadataFieldIdentifierSelected',
  FieldFormat = 'ComplianceMetadataFieldFormatSelected',
  Save = 'SaveComplianceMetadata'
}

export { ComplianceEvent };
