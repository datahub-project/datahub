export type DataProductBuilderState = {
    name: string;
    id?: string;
    description?: string;
};

export type DataProductBuilderFormProps = {
    builderState: DataProductBuilderState;
    updateBuilderState: (newState: DataProductBuilderState) => void;
};
