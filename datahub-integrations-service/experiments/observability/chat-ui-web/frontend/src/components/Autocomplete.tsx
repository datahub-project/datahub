import { useState, useRef, useEffect } from 'react';

interface AutocompleteProps {
  options: string[];
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  disabled?: boolean;
  label?: string;
}

export function Autocomplete({
  options,
  value,
  onChange,
  placeholder = 'Type to search...',
  disabled = false,
  label,
}: AutocompleteProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [inputValue, setInputValue] = useState(value);
  const [filteredOptions, setFilteredOptions] = useState<string[]>(options);
  const [highlightedIndex, setHighlightedIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Update input value when prop changes
  useEffect(() => {
    setInputValue(value);
  }, [value]);

  // Update filtered options when options change
  useEffect(() => {
    filterOptions(inputValue);
  }, [options]);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(event.target as Node) &&
        inputRef.current &&
        !inputRef.current.contains(event.target as Node)
      ) {
        setIsOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const filterOptions = (searchValue: string) => {
    if (!searchValue) {
      setFilteredOptions(options);
      return;
    }

    const search = searchValue.toLowerCase();
    const filtered = options.filter((option) =>
      option.toLowerCase().includes(search)
    );
    setFilteredOptions(filtered);
    setHighlightedIndex(0);
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newValue = e.target.value;
    setInputValue(newValue);
    filterOptions(newValue);
    setIsOpen(true);
  };

  const handleOptionClick = (option: string) => {
    setInputValue(option);
    onChange(option);
    setIsOpen(false);
  };

  const handleInputFocus = () => {
    setIsOpen(true);
    filterOptions(inputValue);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (!isOpen) {
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        setIsOpen(true);
        e.preventDefault();
      }
      return;
    }

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setHighlightedIndex((prev) =>
          prev < filteredOptions.length - 1 ? prev + 1 : prev
        );
        break;
      case 'ArrowUp':
        e.preventDefault();
        setHighlightedIndex((prev) => (prev > 0 ? prev - 1 : 0));
        break;
      case 'Enter':
        e.preventDefault();
        if (filteredOptions[highlightedIndex]) {
          handleOptionClick(filteredOptions[highlightedIndex]);
        }
        break;
      case 'Escape':
        e.preventDefault();
        setIsOpen(false);
        break;
      case 'Tab':
        setIsOpen(false);
        break;
    }
  };

  return (
    <div className="autocomplete-container">
      {label && <label className="autocomplete-label">{label}</label>}
      <div className="autocomplete-wrapper">
        <input
          ref={inputRef}
          type="text"
          className="autocomplete-input"
          value={inputValue}
          onChange={handleInputChange}
          onFocus={handleInputFocus}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          disabled={disabled}
          autoComplete="off"
        />
        {isOpen && !disabled && filteredOptions.length > 0 && (
          <div ref={dropdownRef} className="autocomplete-dropdown">
            {filteredOptions.map((option, index) => (
              <div
                key={option}
                className={`autocomplete-option ${
                  index === highlightedIndex ? 'highlighted' : ''
                } ${option === value ? 'selected' : ''}`}
                onClick={() => handleOptionClick(option)}
                onMouseEnter={() => setHighlightedIndex(index)}
              >
                {option}
              </div>
            ))}
          </div>
        )}
        {isOpen && !disabled && filteredOptions.length === 0 && inputValue && (
          <div ref={dropdownRef} className="autocomplete-dropdown">
            <div className="autocomplete-option autocomplete-no-results">
              No matches found
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
