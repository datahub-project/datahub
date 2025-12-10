export interface BuildChatContextParams {
    isEditing: boolean;
    sourceUrn?: string;
    sourceType?: string;
    sourceName?: string;
    currentStep?: string;
    stepContext?: string;
}

export function buildIngestionSourceChatContext({
    isEditing,
    sourceUrn,
    sourceType,
    sourceName,
    currentStep,
    stepContext,
}: BuildChatContextParams): string {
    const parts: string[] = [];

    // Base context
    if (isEditing) {
        parts.push(`The user is editing an existing ingestion source`);
        if (sourceUrn) {
            parts.push(`with URN: ${sourceUrn}`);
        }
    } else {
        parts.push(`The user is creating a new ingestion source`);
    }

    // Add source type if available
    if (sourceType) {
        parts.push(`. The source type is "${sourceType}".`);
    } else {
        parts.push('.');
    }

    // Add source name if available
    if (sourceName) {
        parts.push(` The source name is "${sourceName}".`);
    }

    // Add current step context
    if (currentStep) {
        parts.push(` The user is currently on the "${currentStep}" step.`);
        if (stepContext) {
            parts.push(` This is the context of what that step is meant for: ${stepContext}`);
        }
    }

    // Add helpful context about what the user might need
    parts.push(
        ' This is a configuration context where the user may ask questions about connection details, authentication, scheduling, or troubleshooting configuration issues.',
    );

    return parts.join('');
}
