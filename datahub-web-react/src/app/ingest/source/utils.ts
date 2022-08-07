import YAML from 'yamljs';
import { CheckCircleOutlined, CloseCircleOutlined, LoadingOutlined } from '@ant-design/icons';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../entity/shared/constants';
import { SOURCE_TEMPLATE_CONFIGS } from './conf/sources';

export const sourceTypeToIconUrl = (type: string) => {
    return SOURCE_TEMPLATE_CONFIGS.find((config) => config.type === type)?.logoUrl;
};

export const getSourceConfigs = (sourceType: string) => {
    const sourceConfigs = SOURCE_TEMPLATE_CONFIGS.find((configs) => configs.type === sourceType);
    if (!sourceConfigs) {
        throw new Error(`Failed to find source configs with source type ${sourceType}`);
    }
    return sourceConfigs;
};

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

export const RUNNING = 'RUNNING';
export const SUCCESS = 'SUCCESS';
export const FAILURE = 'FAILURE';
export const CANCELLED = 'CANCELLED';

export const getExecutionRequestStatusIcon = (status: string) => {
    return (
        (status === RUNNING && LoadingOutlined) ||
        (status === SUCCESS && CheckCircleOutlined) ||
        (status === FAILURE && CloseCircleOutlined) ||
        (status === CANCELLED && CloseCircleOutlined) ||
        undefined
    );
};

export const getExecutionRequestStatusDisplayText = (status: string) => {
    return (
        (status === RUNNING && 'Running') ||
        (status === SUCCESS && 'Succeeded') ||
        (status === FAILURE && 'Failed') ||
        (status === CANCELLED && 'Cancelled') ||
        status
    );
};

export const getExecutionRequestStatusDisplayColor = (status: string) => {
    return (
        (status === RUNNING && REDESIGN_COLORS.BLUE) ||
        (status === SUCCESS && 'green') ||
        (status === FAILURE && 'red') ||
        (status === CANCELLED && ANTD_GRAY[9]) ||
        ANTD_GRAY[7]
    );
};
