#!/bin/bash

# =============================================================================
# Test Data Generation Script for DataHub PostgreSQL Setup (Profiles)
# =============================================================================
# 
# This script generates test data for the DataHub graph schema by:
# 1. Prompting the user to select a data scale
# 2. Setting the appropriate custom parameters for vertices, edges, and edge types
# 3. Executing the unified test data generation SQL script
# 4. Verifying the generated data
#
# The script now uses custom parameters instead of predefined scales, giving
# full control over the exact counts generated.
# =============================================================================

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-datahub}"
DB_NAME="${DB_NAME:-datahub}"
DB_PASSWORD="${DB_PASSWORD:-datahub}"
SQL_FILE="generate_test_data_unified.sql"
VERIFY_FILE="verify_test_data_unified.sql"

# Set default password for psql
export PGPASSWORD="$DB_PASSWORD"

# Function to display usage information
show_usage() {
    echo -e "${BLUE}Usage:${NC}"
    echo -e "  $0 [options]"
    echo -e ""
    echo -e "${BLUE}Options:${NC}"
    echo -e "  -h, --help          Show this help message"
    echo -e "  -s, --scale SCALE   Set data scale (standard, scaled, custom)"
    echo -e "  -v, --vertices N    Set custom vertex count"
    echo -e "  -e, --edges N       Set custom edge count"
    echo -e "  -t, --types N       Set custom edge type count"
    echo -e ""
    echo -e "${BLUE}Examples:${NC}"
    echo -e "  $0                                    # Interactive mode"
    echo -e "  $0 -s standard                       # Standard scale"
    echo -e "  $0 -s scaled                         # Scaled mode"
    echo -e "  $0 -v 5000 -e 25000 -t 4            # Custom counts"
    echo -e ""
    echo -e "${BLUE}Scale Options:${NC}"
    echo -e "  standard: 3 edge types, 1K vertices, 5K edges"
    echo -e "  scaled:   8 edge types, 100K vertices, 500K edges"
    echo -e "  custom:   User-defined counts"
}

# Function to generate test data
generate_test_data() {
    local scale_name="$1"
    local vertex_count="$2"
    local edge_count="$3"
    local edge_type_count="$4"
    
    echo -e "${BLUE}Generating test data with scale: $scale_name${NC}"
    echo -e "${BLUE}Configuration:${NC}"
    echo -e "  Vertices: $vertex_count"
    echo -e "  Edges: $edge_count"
    echo -e "  Edge Types: $edge_type_count"
    
    # Create temporary SQL file with custom parameters
    cat > /tmp/scale_setting.sql << EOF
-- Set custom parameters for test data generation
SET datahub.custom_vertices = '$vertex_count';
SET datahub.custom_edges = '$edge_count';
SET datahub.custom_edge_types = '$edge_type_count';
EOF
    
    echo -e "${BLUE}Executing SQL script: $SQL_FILE${NC}"
    
    if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f /tmp/scale_setting.sql -f "$SQL_FILE"; then
        echo -e "${GREEN}Test data generation completed successfully!${NC}"
    else
        echo -e "${RED}Test data generation failed!${NC}"
        rm -f /tmp/scale_setting.sql
        exit 1
    fi
}

# Function to verify test data
verify_test_data() {
    echo -e "${BLUE}Verifying generated test data...${NC}"
    
    if [ ! -f "$VERIFY_FILE" ]; then
        echo -e "${RED}Verification file not found: $VERIFY_FILE${NC}"
        rm -f /tmp/scale_setting.sql
        exit 1
    fi
    
    echo -e "${BLUE}Using verification file: $VERIFY_FILE${NC}"
    
    # Pass the same scale setting to verification
    if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f /tmp/scale_setting.sql -f "$VERIFY_FILE"; then
        echo -e "${GREEN}Test data verification completed!${NC}"
    else
        echo -e "${RED}Test data verification failed!${NC}"
        rm -f /tmp/scale_setting.sql
        exit 1
    fi
    
    # Clean up temporary scale setting file after verification
    rm -f /tmp/scale_setting.sql
}

# Function to prompt for scale selection
prompt_for_scale() {
    echo -e "${BLUE}Choose data scale:${NC}"
    echo -e "  1) Standard: 3 edge types, 1K vertices, 5K edges"
    echo -e "  2) Scaled: 8 edge types, 100K vertices, 500K edges"
    echo -e "  3) Custom scale"
    echo -e ""
    read -p "Enter your choice (1-3): " choice
    
    case $choice in
        1)
            SCALE_NAME="standard"
            SCALE_DESC="3 edge types, 1K vertices, 5K edges"
            VERTEX_COUNT=1000
            EDGE_COUNT=5000
            EDGE_TYPE_COUNT=3
            ;;
        2)
            SCALE_NAME="scaled"
            SCALE_DESC="8 edge types, 100K vertices, 500K edges"
            VERTEX_COUNT=100000
            EDGE_COUNT=500000
            EDGE_TYPE_COUNT=8
            ;;
        3)
            SCALE_NAME="custom"
            SCALE_DESC="User-defined counts"
            echo -e "${BLUE}Enter custom parameters:${NC}"
            read -p "Number of vertices: " VERTEX_COUNT
            read -p "Number of edges: " EDGE_COUNT
            read -p "Number of edge types (1-32): " EDGE_TYPE_COUNT
            
            # Validate inputs
            if ! [[ "$VERTEX_COUNT" =~ ^[0-9]+$ ]] || [ "$VERTEX_COUNT" -lt 1 ]; then
                echo -e "${RED}Invalid vertex count. Must be a positive integer.${NC}"
                exit 1
            fi
            
            if ! [[ "$EDGE_COUNT" =~ ^[0-9]+$ ]] || [ "$EDGE_COUNT" -lt 1 ]; then
                echo -e "${RED}Invalid edge count. Must be a positive integer.${NC}"
                exit 1
            fi
            
            if ! [[ "$EDGE_TYPE_COUNT" =~ ^[0-9]+$ ]] || [ "$EDGE_TYPE_COUNT" -lt 1 ] || [ "$EDGE_TYPE_COUNT" -gt 32 ]; then
                echo -e "${RED}Invalid edge type count. Must be between 1 and 32.${NC}"
                exit 1
            fi
            ;;
        *)
            echo -e "${YELLOW}Invalid choice, using standard scale${NC}"
            SCALE_NAME="standard"
            SCALE_DESC="3 edge types, 1K vertices, 5K edges"
            VERTEX_COUNT=1000
            EDGE_COUNT=5000
            EDGE_TYPE_COUNT=3
            ;;
    esac
    
    echo -e "${GREEN}Selected scale: $SCALE_NAME${NC}"
    echo -e "${BLUE}Description: $SCALE_DESC${NC}"
}

# Function to show cleanup command
show_cleanup_command() {
    local vertex_count="$1"
    local edge_count="$2"
    
    echo -e ""
    echo -e "${BLUE}=== Cleanup Information ===${NC}"
    echo -e "${YELLOW}To clean up test data, run:${NC}"
    
    # Determine if this is a large dataset
    if [ "$vertex_count" -gt 10000 ] || [ "$edge_count" -gt 50000 ]; then
        echo -e "${RED}WARNING: Large dataset detected! Cleanup may take several minutes.${NC}"
    fi
    
    echo -e "  psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \\"
    echo -e "    \"TRUNCATE TABLE metadata_graph_cc_computation_log CASCADE; \\"
    echo -e "     TRUNCATE TABLE metadata_graph_cc_vertices CASCADE; \\"
    echo -e "     TRUNCATE TABLE metadata_graph_cc CASCADE; \\"
    echo -e "     TRUNCATE TABLE metadata_graph_cc_definitions CASCADE; \\"
    echo -e "     TRUNCATE TABLE metadata_graph_edges CASCADE; \\"
    echo -e "     TRUNCATE TABLE metadata_graph_vertices CASCADE; \\"
    echo -e "     TRUNCATE TABLE metadata_graph_edge_types CASCADE;\""
    echo -e ""
}

# Function to show connected components generation commands
show_cc_generation_commands() {
    local vertex_count="$1"
    local edge_count="$2"
    
    echo -e "${BLUE}=== Connected Components Generation ===${NC}"
    echo -e "${YELLOW}To generate connected components for analysis, run:${NC}"
    
    # Determine if this is a large dataset
    if [ "$vertex_count" -gt 10000 ] || [ "$edge_count" -gt 50000 ]; then
        echo -e "${RED}WARNING: Large dataset detected! CC generation may take several minutes.${NC}"
        echo -e "${YELLOW}Consider running CC calculations in smaller batches or during off-peak hours.${NC}"
    fi
    
    echo -e ""
    echo -e "${CYAN}1. Generate CCs for DEPENDS_ON relationships (edge type 1):${NC}"
    echo -e "  psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \\"
    echo -e "    \"SELECT dh_calculate_ccs_full(1);\""
    echo -e ""
    
    echo -e "${CYAN}2. Generate CCs for OWNED_BY relationships (edge type 2):${NC}"
    echo -e "  psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \\"
    echo -e "    \"SELECT dh_calculate_ccs_full(2);\""
    echo -e ""
    
    echo -e "${CYAN}3. Generate CCs for CONTAINS relationships (edge type 3):${NC}"
    echo -e "  psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \\"
    echo -e "    \"SELECT dh_calculate_ccs_full(3);\""
    echo -e ""
    
    echo -e "${CYAN}4. Check CC generation status:${NC}"
    echo -e "  psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \\"
    echo -e "    \"SELECT cc_def_id, computation_type, started_at, completed_at, execution_time_ms \\"
    echo -e "     FROM metadata_graph_cc_computation_log \\"
    echo -e "     ORDER BY cc_def_id;\""
    echo -e ""
    
    echo -e "${CYAN}5. View CC statistics:${NC}"
    echo -e "  psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \\"
    echo -e "    \"SELECT cc.name, COUNT(DISTINCT vcc.cc_id) as active_ccs, \\"
    echo -e "            COUNT(DISTINCT vcc.vertex_id) as total_vertices \\"
    echo -e "     FROM metadata_graph_cc_definitions cc \\"
    echo -e "     LEFT JOIN metadata_graph_cc_vertices vcc ON cc.id = vcc.cc_def_id \\"
    echo -e "     GROUP BY cc.id, cc.name \\"
    echo -e "     ORDER BY cc.id;\""
    echo -e ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -s|--scale)
            SCALE_NAME="$2"
            shift 2
            ;;
        -v|--vertices)
            VERTEX_COUNT="$2"
            shift 2
            ;;
        -e|--edges)
            EDGE_COUNT="$2"
            shift 2
            ;;
        -t|--types)
            EDGE_TYPE_COUNT="$2"
            shift 2
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    echo -e "${BLUE}=== DataHub Test Data Generation ===${NC}"
    echo -e "${BLUE}Database: $DB_NAME@$DB_HOST:$DB_PORT${NC}"
    echo -e ""
    
    # Check if SQL file exists
    if [ ! -f "$SQL_FILE" ]; then
        echo -e "${RED}SQL file not found: $SQL_FILE${NC}"
        echo -e "${YELLOW}Please run this script from the postgres-setup directory${NC}"
        exit 1
    fi
    
    # If scale parameters were provided via command line, use them
    if [ -n "$SCALE_NAME" ] && [ -n "$VERTEX_COUNT" ] && [ -n "$EDGE_COUNT" ] && [ -n "$EDGE_TYPE_COUNT" ]; then
        echo -e "${BLUE}Using command line parameters${NC}"
    else
        # Prompt for scale selection
        prompt_for_scale
    fi
    
    # Generate test data
    generate_test_data "$SCALE_NAME" "$VERTEX_COUNT" "$EDGE_COUNT" "$EDGE_TYPE_COUNT"
    
    # Verify test data
    verify_test_data
    
    # Show cleanup command
    show_cleanup_command "$VERTEX_COUNT" "$EDGE_COUNT"
    
    # Show CC generation commands
    show_cc_generation_commands "$VERTEX_COUNT" "$EDGE_COUNT"
    
    echo -e "${GREEN}=== Test data generation and verification completed! ===${NC}"
}

# Run main function
main "$@"
