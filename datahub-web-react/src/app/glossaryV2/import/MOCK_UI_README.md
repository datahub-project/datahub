# CSV Import Mock UI

This branch contains a UI-only version of the CSV import functionality with mock data and simplified logic hooks.

## 🎯 Purpose

This mock UI allows you to:
- **Develop UI components** without complex business logic
- **Test UI interactions** with realistic mock data
- **Prototype new features** quickly
- **Design user flows** without backend dependencies

## 📁 Structure

### Mock Data
```
shared/mocks/
├── mockData.ts                    # All mock data (entities, progress, file upload states)
```

### Mock Hooks
```
shared/hooks/
├── useMockImportProcessing.ts     # Mock import orchestration
├── useMockCsvProcessing.ts        # Mock CSV parsing
├── useMockEntityManagement.ts     # Mock entity operations
├── useMockEntityComparison.ts     # Mock entity comparison
└── useMockGraphQLOperations.ts    # Mock GraphQL operations
```

### Mock UI Components
```
WizardPage/
├── WizardPage.mock.tsx           # Main mock UI component
└── DropzoneTable/
    └── useMockFileUpload.ts      # Mock file upload logic
```

## 🚀 How to Use

### 1. Access the Mock UI
Navigate to: `http://localhost:3000/glossaryV2/import/mock`

### 2. Mock Data Included
- **4 sample entities** with different statuses (new, updated, existing, conflict)
- **Realistic ownership data** (users and groups)
- **Parent-child relationships** (Business Terms → Customer ID, etc.)
- **Custom properties** (sensitivity, classification)
- **Progress states** (idle, processing, completed, with errors)

### 3. Interactive Features
- **File Upload**: Drag & drop CSV files (mock processing)
- **Entity Editing**: Click cells to edit inline
- **Status Filtering**: Filter by new/updated/existing/conflict
- **Search**: Search by name or description
- **Pagination**: Navigate through large datasets
- **Import Progress**: Simulated import with progress updates

## 🎨 UI Components You Can Develop

### 1. **Entity Table**
- Inline editing
- Status indicators
- Action buttons (View Details, Diff)
- Sorting and filtering

### 2. **File Upload**
- Drag & drop interface
- Progress indicators
- Error handling
- File validation

### 3. **Import Progress**
- Progress bars
- Status messages
- Error/warning display
- Pause/resume/cancel controls

### 4. **Entity Details Modal**
- Form fields for all entity properties
- Validation feedback
- Save/cancel actions

### 5. **Diff Modal**
- Side-by-side comparison
- Change highlighting
- Conflict resolution

## 🔧 Customizing Mock Data

### Add More Entities
Edit `shared/mocks/mockData.ts`:
```typescript
export const mockEntities: Entity[] = [
  // Add your entities here
  {
    id: '5',
    name: 'New Entity',
    type: 'glossaryTerm',
    // ... other properties
  }
];
```

### Modify Progress States
```typescript
export const mockProgressStates = {
  // Add custom progress states
  customState: {
    isProcessing: true,
    currentOperation: 'Custom operation...',
    // ... other properties
  }
};
```

### Update File Upload States
```typescript
export const mockFileUploadStates = {
  // Add custom upload states
  customUpload: {
    isDragOver: false,
    isUploading: true,
    // ... other properties
  }
};
```

## 🧪 Testing UI Components

### 1. **Unit Tests**
Test individual components with mock data:
```typescript
import { render, screen } from '@testing-library/react';
import { WizardPageMock } from './WizardPage.mock';

test('renders entity table', () => {
  render(<WizardPageMock />);
  expect(screen.getByText('Customer ID')).toBeInTheDocument();
});
```

### 2. **Integration Tests**
Test component interactions:
```typescript
test('allows editing entity name', () => {
  render(<WizardPageMock />);
  const nameCell = screen.getByText('Customer ID');
  fireEvent.click(nameCell);
  // Test editing functionality
});
```

### 3. **Visual Testing**
Use tools like Storybook or Chromatic to test UI variations.

## 🔄 Switching Back to Real Logic

When you're ready to integrate with real business logic:

1. **Replace mock hooks** with real hooks:
   ```typescript
   // Change from:
   import { useMockImportProcessing } from '../shared/hooks/useMockImportProcessing';
   
   // To:
   import { useImportProcessing } from '../shared/hooks/useImportProcessing';
   ```

2. **Update data sources**:
   ```typescript
   // Change from:
   const { entities } = useImportState();
   
   // To:
   const { entities } = useRealImportState();
   ```

3. **Add real GraphQL operations**:
   ```typescript
   // Change from:
   const { executePatchEntitiesMutation } = useMockGraphQLOperations();
   
   // To:
   const { executePatchEntitiesMutation } = useGraphQLOperations();
   ```

## 📝 Notes

- **No real data persistence** - all changes are in-memory
- **No real GraphQL calls** - all operations are mocked
- **No real file processing** - CSV parsing is simulated
- **Perfect for UI development** - focus on user experience without backend complexity

## 🎯 Next Steps

1. **Develop UI components** using the mock data
2. **Test user interactions** and flows
3. **Refine the design** based on user feedback
4. **Integrate with real logic** when ready
5. **Add comprehensive tests** for the UI components

This mock UI gives you a solid foundation for developing the CSV import interface without getting bogged down in complex business logic!
