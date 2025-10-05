import { useState, useCallback } from 'react';
import { ValidationError, ValidationWarning } from '../../glossary.types';

export interface AppError {
  id: string;
  type: 'network' | 'validation' | 'import' | 'csv' | 'graphql' | 'unknown';
  message: string;
  details?: string;
  timestamp: Date;
  retryable: boolean;
  context?: Record<string, any>;
}

export interface ErrorRecoveryAction {
  id: string;
  label: string;
  action: () => void;
  type: 'retry' | 'fix' | 'skip' | 'cancel';
}

export interface UseErrorHandlingReturn {
  errors: AppError[];
  warnings: ValidationWarning[];
  addError: (error: Omit<AppError, 'id' | 'timestamp'>) => void;
  addWarning: (warning: ValidationWarning) => void;
  removeError: (errorId: string) => void;
  clearErrors: () => void;
  clearWarnings: () => void;
  getRecoveryActions: (error: AppError) => ErrorRecoveryAction[];
  handleError: (error: unknown, context?: Record<string, any>) => void;
  isErrorVisible: (errorId: string) => boolean;
  dismissError: (errorId: string) => void;
}

export const useErrorHandling = (): UseErrorHandlingReturn => {
  const [errors, setErrors] = useState<AppError[]>([]);
  const [warnings, setWarnings] = useState<ValidationWarning[]>([]);
  const [dismissedErrors, setDismissedErrors] = useState<Set<string>>(new Set());

  const addError = useCallback((error: Omit<AppError, 'id' | 'timestamp'>) => {
    const newError: AppError = {
      ...error,
      id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      timestamp: new Date(),
    };

    setErrors(prev => [...prev, newError]);
  }, []);

  const addWarning = useCallback((warning: ValidationWarning) => {
    setWarnings(prev => [...prev, warning]);
  }, []);

  const removeError = useCallback((errorId: string) => {
    setErrors(prev => prev.filter(error => error.id !== errorId));
  }, []);

  const clearErrors = useCallback(() => {
    setErrors([]);
    setDismissedErrors(new Set());
  }, []);

  const clearWarnings = useCallback(() => {
    setWarnings([]);
  }, []);

  const dismissError = useCallback((errorId: string) => {
    setDismissedErrors(prev => new Set([...prev, errorId]));
  }, []);

  const isErrorVisible = useCallback((errorId: string) => {
    return !dismissedErrors.has(errorId);
  }, [dismissedErrors]);

  const getRecoveryActions = useCallback((error: AppError): ErrorRecoveryAction[] => {
    const actions: ErrorRecoveryAction[] = [];

    if (error.retryable) {
      actions.push({
        id: 'retry',
        label: 'Retry',
        action: () => {
          // Retry logic would be implemented by the calling component
          removeError(error.id);
        },
        type: 'retry',
      });
    }

    switch (error.type) {
      case 'validation':
        actions.push({
          id: 'fix',
          label: 'Fix Data',
          action: () => {
            // Navigate to validation errors
            removeError(error.id);
          },
          type: 'fix',
        });
        break;

      case 'csv':
        actions.push({
          id: 'fix',
          label: 'Re-upload File',
          action: () => {
            // Trigger file re-upload
            removeError(error.id);
          },
          type: 'fix',
        });
        break;

      case 'network':
        actions.push({
          id: 'retry',
          label: 'Retry Connection',
          action: () => {
            // Retry network operation
            removeError(error.id);
          },
          type: 'retry',
        });
        break;
    }

    actions.push({
      id: 'dismiss',
      label: 'Dismiss',
      action: () => dismissError(error.id),
      type: 'cancel',
    });

    return actions;
  }, [removeError, dismissError]);

  const handleError = useCallback((error: unknown, context?: Record<string, any>) => {
    let appError: Omit<AppError, 'id' | 'timestamp'>;

    if (error instanceof Error) {
      // Network errors
      if (error.message.includes('fetch') || error.message.includes('network')) {
        appError = {
          type: 'network',
          message: 'Network connection failed',
          details: error.message,
          retryable: true,
          context,
        };
      }
      // GraphQL errors
      else if (error.message.includes('GraphQL') || error.message.includes('query')) {
        appError = {
          type: 'graphql',
          message: 'GraphQL operation failed',
          details: error.message,
          retryable: true,
          context,
        };
      }
      // Validation errors
      else if (error.message.includes('validation') || error.message.includes('required')) {
        appError = {
          type: 'validation',
          message: 'Data validation failed',
          details: error.message,
          retryable: false,
          context,
        };
      }
      // CSV parsing errors
      else if (error.message.includes('CSV') || error.message.includes('parse')) {
        appError = {
          type: 'csv',
          message: 'CSV parsing failed',
          details: error.message,
          retryable: false,
          context,
        };
      }
      // Import errors
      else if (error.message.includes('import') || error.message.includes('batch')) {
        appError = {
          type: 'import',
          message: 'Import operation failed',
          details: error.message,
          retryable: true,
          context,
        };
      }
      // Generic errors
      else {
        appError = {
          type: 'unknown',
          message: error.message || 'An unexpected error occurred',
          details: error.stack,
          retryable: false,
          context,
        };
      }
    } else if (typeof error === 'string') {
      appError = {
        type: 'unknown',
        message: error,
        retryable: false,
        context,
      };
    } else {
      appError = {
        type: 'unknown',
        message: 'An unexpected error occurred',
        retryable: false,
        context,
      };
    }

    addError(appError);
  }, [addError]);

  return {
    errors,
    warnings,
    addError,
    addWarning,
    removeError,
    clearErrors,
    clearWarnings,
    getRecoveryActions,
    handleError,
    isErrorVisible,
    dismissError,
  };
};
