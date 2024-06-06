export interface ColumnSelectorProps {
    isBulkApplyingFieldPath: boolean;
    setIsBulkApplyingFieldPath: (isBulkApply: boolean) => void;
    selectedFieldPaths: string[];
    setSelectedFieldPaths: (paths: string[]) => void;
}
