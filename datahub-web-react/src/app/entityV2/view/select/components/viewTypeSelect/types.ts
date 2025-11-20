export interface ViewTypeSelectProps {
    publicViews: boolean;
    privateViews: boolean;
    onTypeSelect: (type: string) => void;
    bordered?: boolean;
}
