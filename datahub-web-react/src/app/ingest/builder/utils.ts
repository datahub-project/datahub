import YAML from 'yamljs';

const baseUrl = window.location.origin;
export const defaultRecipe = `source: 
    # Source type, e.g. 'bigquery' 
    type: <source-type> 
    # Source-specific configs 
    config: 
        <source-configs> 
sink: 
    type: "datahub-rest" 
    config: 
        server: "${baseUrl}/api/gms" 
        # The Access Token to use during ingestion. We recommend using Secrets to capture this. 
        token: <access-token>`;

export const yamlToJson = (yaml: string): string => {
    const obj = YAML.parse(yaml);
    const jsonStr = JSON.stringify(obj);
    return jsonStr;
};

export const jsonToYaml = (json: string): string => {
    const obj = JSON.parse(json);
    const yamlStr = YAML.stringify(obj, 6);
    return yamlStr;
};

export enum IntervalType {
    DAILY,
    WEEKLY,
    MONTHLY,
}
