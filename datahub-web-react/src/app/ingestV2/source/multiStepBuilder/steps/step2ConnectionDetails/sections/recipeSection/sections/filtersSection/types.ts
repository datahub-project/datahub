export interface Filter {
    key: string;
    rule: string | undefined;
    subtype: string | undefined;
    value: string;
}

export interface FilterWithFieldName extends Filter {
    name: string;
}
