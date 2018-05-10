/**
 * Enumerates the available compliance metadata events
 */
enum ComplianceEvent {
  Cancel = 'Cancel Edit Compliance Metadata',
  Next = 'Next Compliance Metadata Step',
  Previous = 'Previous Compliance Metadata Step',
  Edit = 'Begin Edit Compliance Metadata',
  Download = 'Download Compliance Metadata',
  Upload = 'Upload Compliance Metadata',
  SetUnspecifiedAsNone = 'Set Unspecified Fields As None',
  FieldIndentifier = 'Compliance Metadata Field Identifier Selected',
  FieldFormat = 'Compliance Metadata Field Format Selected',
  Save = 'Save Compliance Metadata'
}

export { ComplianceEvent };
