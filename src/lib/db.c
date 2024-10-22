#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> 
#include <string.h>
#include <dirent.h>
#include <stdbool.h>
#include "constants.c"
#include "utils.c"


// Function to create a new database (if it doesn't already exist)
int createDB(const char *database_name) {
    char sanitized_name[256];
    char *filename = "";

    sanitize_str(database_name, sanitized_name, sizeof(sanitized_name), ".db");
    filename = database_path(sanitized_name);
    // Check if the database file already exists
    if (access(filename, F_OK) == 0) {
        return 0; // File exists, return 0
    }
    // If the database doesn't exist, create a new one
    writeToFile(filename, DB_HEADER);
    appendToFile(filename, concat("\nName: ",sanitized_name));
    appendToFile(filename, DB_BODY);
    free(filename);
    // Insert Version Data
    return 1; // Indicate that the database was created successfully
}

// Show DB
char *listDB(const char *directory) {
    struct dirent *entry;
    DIR *dp = opendir(directory);

    if (dp == NULL) {
        perror("Unable to open directory");
        return NULL;
    }

    // Initializing the JSON response structure
    char *json = (char *)malloc(1024); // Allocate some initial space
    strcpy(json, "{\"database\":[");

    int firstFile = 1; // To handle the commas correctly

    while ((entry = readdir(dp))) {
        // Check if the file has a ".db" extension
        if (strstr(entry->d_name, ".db") != NULL) {
            if (!firstFile) {
                strcat(json, ",");
            }
            firstFile = 0;
            strcat(json, "\"");
            strcat(json, entry->d_name);
            strcat(json, "\"");
        }
    }
    closedir(dp);
    // Close the JSON array and structure
    strcat(json, "]}");
    return json;
}

// Delete DB
int deleteDB(const char *database_name) {
    char *filepath = "";
    filepath = database_path(database_name);
    // Attempt to delete the file
    if (remove(filepath) == 0) {
        return 0;  // File successfully deleted
    } else {
        perror("Error deleting file");
        return 1;  // File deletion failed
    }
    free(filepath);
}

// Function to generate schema string with sanitized table name
char* generateSchema(const char *table_name, const char *columns[], const char *types[], int column_count) {
    // Buffer to store the sanitized table name
    char sanitized_name[256];

    // Sanitize the table name (without any extension like ".db")
    sanitize_str(table_name, sanitized_name, sizeof(sanitized_name), NULL);

    // Buffer to store the result (dynamically allocated)
    char *result = (char *)malloc(1024 * sizeof(char)); // Adjust size as needed
    if (result == NULL) {
        perror("Memory allocation failed");
        return NULL;
    }

    // Initialize the result buffer
    strcpy(result, "");

    // Append #Table: <sanitized_table_name>
    strcat(result, "# Table: ");
    strcat(result, sanitized_name);
    strcat(result, "\n");

    // Append #Columns: <columns>
    strcat(result, "# Columns: ");
    for (int i = 0; i < column_count; i++) {
        strcat(result, columns[i]);
        strcat(result, " ");
        strcat(result, types[i]);
        if (i != column_count - 1) {
            strcat(result, ", ");
        }
    }
    strcat(result, "\n\n");

    // Append individual column definitions
    for (int i = 0; i < column_count; i++) {
        strcat(result, columns[i]);
        strcat(result, " ");
        strcat(result, types[i]);
        strcat(result, "\n");
    }
      return result; // Caller should free this memory
}

int tableExists(const char *filename, const char *table_name) {
    char sanitized_table[256];
    sanitize_str(table_name, sanitized_table, sizeof(sanitized_table), NULL);

    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        perror("Unable to open file for reading");
        return 0; // File does not exist or can't be opened
    }

    char buffer[256];
    while (fgets(buffer, sizeof(buffer), file) != NULL) {
        // Check if the line contains the sanitized table name
        if (strstr(buffer, sanitized_table) != NULL) {
            fclose(file);
            return 1; // Schema already exists
        }
    }

    fclose(file);
    return 0; // Schema not found
}

int createTable(const char *database_name, const char *table_name, const char *columns[], const char *types[], int column_count) {
    char *filename = "";
    filename = database_path(database_name);

    // Check if the table already exists
    if (tableExists(filename, table_name)) {
        return 0; // Indicate that the table already exists
    }

    // Generate the schema using the table_name, columns, and types
    char *schema = generateSchema(table_name, columns, types, column_count);
    if (schema == NULL) {
        return 0; // Return 0 if schema generation failed
    }

    // Open the file to read and update content
    FILE *file = fopen(filename, "r+"); // Open for reading and writing
    if (file == NULL) {
        perror("Unable to open file for reading and writing");
        free(schema); // Free the dynamically allocated memory
        return 0;
    }

    // Read the entire content of the file
    fseek(file, 0, SEEK_END); // Move to the end of the file
    size_t content_size = ftell(file); // Get the size of the file
    rewind(file); // Move back to the beginning of the file

    char *file_content = (char *)malloc(content_size + 1); // Allocate memory for file content
    if (file_content == NULL) {
        perror("Unable to allocate memory for file content");
        fclose(file);
        free(schema); // Free the dynamically allocated memory
        return 0;
    }

    fread(file_content, 1, content_size, file); // Read the entire file content
    file_content[content_size] = '\0'; // Null-terminate the string

    // Find the positions of [TABLE_BEGIN] and [TABLE_END]
    const char *begin_tag = "[TABLE_BEGIN]";
    const char *end_tag = "[TABLE_END]";
    char *begin_pos = strstr(file_content, begin_tag);
    char *end_pos = strstr(file_content, end_tag);

    // First append the schema between [TABLE_BEGIN] and [TABLE_END]
    if (begin_pos != NULL && end_pos != NULL && begin_pos < end_pos) {
        // Create new content for schema insertion
        size_t new_size = content_size + strlen(schema) + 1;
        char *new_content = (char *)malloc(new_size);
        if (new_content == NULL) {
            perror("Unable to allocate memory for new content");
            free(file_content);
            fclose(file);
            free(schema);
            return 0;
        }

        // Copy everything before [TABLE_END]
        size_t prefix_length = end_pos - file_content; // Length of content before [TABLE_END]
        strncpy(new_content, file_content, prefix_length);
        new_content[prefix_length] = '\0';

        // Append the schema after [TABLE_BEGIN]
        strcat(new_content, "\n");
        strcat(new_content, schema);
        strcat(new_content, "\n");
        strcat(new_content, end_pos); // Append everything after [TABLE_END]

        free(file_content); // Free the old content as we now have new content
        file_content = new_content; // Update file_content to new_content with schema inserted
    } else {
        free(file_content);
        fclose(file);
        free(schema);
        return 0;
    }

    free(schema); // Free schema memory as it's already inserted

    // Now find the positions of [TABLE_VALUE_BEGIN] and [TABLE_VALUE_END] to append table info
    const char *table_value_begin = "[TABLE_VALUE_BEGIN]";
    const char *table_value_end = "[TABLE_VALUE_END]";
    char *table_value_begin_pos = strstr(file_content, table_value_begin);
    char *table_value_end_pos = strstr(file_content, table_value_end);

    if (table_value_begin_pos != NULL && table_value_end_pos != NULL && table_value_begin_pos < table_value_end_pos) {
        // Prepare table info string
        char table_info[256];
        snprintf(table_info, sizeof(table_info), "# Table: %s\n\n", table_name);

        // Allocate memory for new content with table info
        size_t new_size_with_table_info = strlen(file_content) + strlen(table_info) + 1;
        char *new_content_with_table_info = (char *)malloc(new_size_with_table_info);
        if (new_content_with_table_info == NULL) {
            perror("Unable to allocate memory for new content with table info");
            free(file_content);
            fclose(file);
            return 0;
        }

        // Copy everything before [TABLE_VALUE_END]
        size_t table_value_prefix_length = table_value_end_pos - file_content;
        strncpy(new_content_with_table_info, file_content, table_value_prefix_length);
        new_content_with_table_info[table_value_prefix_length] = '\0'; // Null-terminate the string

        // Append the table info between [TABLE_VALUE_BEGIN] and [TABLE_VALUE_END]
        strcat(new_content_with_table_info, table_info);
        strcat(new_content_with_table_info, table_value_end_pos); // Append the rest of the content

        // Write the new content with both schema and table info back to the file
        rewind(file); // Move back to the beginning of the file
        fputs(new_content_with_table_info, file); // Write the final content
        ftruncate(fileno(file), ftell(file)); // Truncate the file to the new length

        // Clean up
        free(new_content_with_table_info);
    } else {
        free(file_content);
        fclose(file);
        return 0;
    }

    free(file_content); // Free the final content memory
    fclose(file); // Close the file
    free(filename);
    return 1; // Indicate success
}

int insertTableValues(const char *database_name, const char *table_name, const char *values) {
    char *filename = "";
    filename = database_path(database_name);

    if (tableExists(filename, table_name) <= 0) {
        return 0; // Indicate that the table does not exists
    }

    // Open the file to read and update content
    FILE *file = fopen(filename, "r+"); // Open for reading and writing
    if (file == NULL) {
        perror("Unable to open file for reading and writing");
        return 0;
    }

    // Read the entire content of the file
    fseek(file, 0, SEEK_END); // Move to the end of the file
    size_t content_size = ftell(file); // Get the size of the file
    rewind(file); // Move back to the beginning of the file

    char *file_content = (char *)malloc(content_size + 1); // Allocate memory for file content
    if (file_content == NULL) {
        perror("Unable to allocate memory for file content");
        fclose(file);
        return 0;
    }

    fread(file_content, 1, content_size, file); // Read the entire file content
    file_content[content_size] = '\0'; // Null-terminate the string

    // Find the positions of [TABLE_VALUE_BEGIN] and [TABLE_VALUE_END]
    const char *table_value_begin = "[TABLE_VALUE_BEGIN]";
    const char *table_value_end = "[TABLE_VALUE_END]";
    char *table_value_begin_pos = strstr(file_content, table_value_begin);
    char *table_value_end_pos = strstr(file_content, table_value_end);

    if (table_value_begin_pos != NULL && table_value_end_pos != NULL && table_value_begin_pos < table_value_end_pos) {
        // Find the matching table name after [TABLE_VALUE_BEGIN]
        char table_header[256];
        snprintf(table_header, sizeof(table_header), "# Table: %s", table_name);
        char *table_pos = strstr(table_value_begin_pos, table_header);

        if (table_pos != NULL && table_pos < table_value_end_pos) {
            // Find the end of the matched table name block
            table_pos = strchr(table_pos, '\n'); // Move to the end of the line containing the table name

            // Create the new content with the values appended
            size_t new_size = content_size + strlen(values) + 2; // +2 for newline and null terminator
            char *new_content = (char *)malloc(new_size);
            if (new_content == NULL) {
                perror("Unable to allocate memory for new content");
                free(file_content);
                fclose(file);
                return 0;
            }

            // Copy everything up to the table position
            size_t prefix_length = table_pos - file_content + 1; // Include the newline
            strncpy(new_content, file_content, prefix_length);
            new_content[prefix_length] = '\0'; // Null-terminate the string

            // Append the new values
            strcat(new_content, values);
            strcat(new_content, "\n"); // Add a newline after the values

            // Append the rest of the content after the table values
            strcat(new_content, table_pos + 1); // Move one character ahead after the newline

            // Write the new content back to the file
            rewind(file); // Move back to the beginning of the file
            fputs(new_content, file); // Write new content
            ftruncate(fileno(file), ftell(file)); // Truncate the file to the new length

            // Clean up
            free(new_content);
        } 
    }
    free(file_content); // Free the content memory
    fclose(file); // Close the file
    free(filename);
    return 1; // Indicate success
}

char* fetchTableData(const char *database_name, const char *table_name) {
    // Allocate memory for filename
    char *filename = malloc(256);
    if (!filename) {
        perror("Memory allocation failed");
        return NULL;
    }

    strcpy(filename, database_path(database_name));

    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        perror("Unable to open file");
        free(filename);  // Free memory before returning
        return NULL;
    }

    char line[256];
    int in_table_values = 0;
    int found_table = 0;
    int in_table_schema = 0;
    int found_schema = 0;

    // Allocate empty strings for schema_data and table_data
    char *table_data = calloc(1, sizeof(char));
    char *schema_data = calloc(1, sizeof(char));

    if (!table_data || !schema_data) {
        perror("Memory allocation failed");
        free(filename);
        fclose(file);
        return NULL;
    }

    // Read the file line by line
    while (fgets(line, sizeof(line), file)) {
        if (strstr(line, "[TABLE_VALUE_BEGIN]") != NULL) {
            in_table_values = 1;
            continue;
        }
        if (strstr(line, "[TABLE_VALUE_END]") != NULL) {
            in_table_values = 0;
            found_table = 0;
            continue;
        }
        if (strstr(line, "[TABLE_BEGIN]") != NULL) {
            in_table_schema = 1;
            continue;
        }
        if (strstr(line, "[TABLE_END]") != NULL) {
            in_table_schema = 0;
            found_schema = 0;
            continue;
        }

        // Table Schema Section
        if (in_table_schema) {
            if (strstr(line, table_name) != NULL) {
                found_schema = 1;
            } else if (found_schema) {
                if (strstr(line, "# Columns: ") != NULL) {
                    schema_data = concat(schema_data, line);
                }
            }
        }

        // Table Values Section
        if (in_table_values) {
            if (strstr(line, table_name) != NULL) {
                found_table = 1;
            } else if (found_table) {
                if (strstr(line, "# Table: ") == NULL) {
                    table_data = concat(table_data, line);
                }
            }
        }
    }

    fclose(file);
    // If no table values were found, return an empty array "[]"
    if (strcmp(table_data, "") == 0 || strcmp(schema_data, "") == 0 || strlen(table_data) < 2) {
        free(table_data);
        free(schema_data);
        free(filename);
        return strdup("[]");  // Return an empty array
    }

    // Replace unwanted string in schema
    schema_data = replaceString(schema_data, "# Columns: ", "");

    // Split schema into fields
    int count = 0;
    char **result = split_string(schema_data, ",", &count);

    // Allocate memory for fields
    char *fields = calloc(1, sizeof(char));
    if (!fields) {
        perror("Memory allocation failed");
        free(table_data);
        free(schema_data);
        free(filename);
        free(result);
        return NULL;
    }

    // Build fields string
    for (int i = 0; i < count; i++) {
        int count_ = 0;
        char **trim_result = split_string(trim(result[i]), " ", &count_);
        fields = concat(fields, trim_result[0]);
        if (i < count - 1) {
            fields = concat(fields, ",");
        }
        free(trim_result);  // Free the result from split
        free(result[i]);    // Free each token from split
    }

    free(result);  // Free the array of tokens
    free(schema_data);

    // Get the JSON output
    char *json_output = map_fields_to_json(fields, trim(table_data));

    // Free allocated memory
    free(fields);
    free(table_data);
    free(filename);

    // Return the final JSON output
    return json_output;
}

char* fetchFilteredTableData(const char *database_name, const char *table_name, const char *check_field, const char *check_value) {
    // Allocate memory for filename
    char *filename = malloc(256);
    if (!filename) {
        perror("Memory allocation failed");
        return NULL;
    }

    strcpy(filename, database_path(database_name));

    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        perror("Unable to open file");
        free(filename);
        return NULL;
    }

    char line[256];
    int in_table_values = 0;
    int found_table = 0;
    int in_table_schema = 0;
    int found_schema = 0;
    int schema_parsed = 0;  // Ensure schema is only parsed once

    char *table_data = calloc(1, sizeof(char));
    char *schema_data = calloc(1, sizeof(char));

    if (!table_data || !schema_data) {
        perror("Memory allocation failed");
        free(filename);
        fclose(file);
        return NULL;
    }

    // Read the file line by line
    while (fgets(line, sizeof(line), file)) {
        // Only process the schema for the specific table, and stop once it's found and processed
        if (!schema_parsed) {
            if (strstr(line, "[TABLE_BEGIN]") != NULL) {
                in_table_schema = 1;
                continue;
            }
            if (strstr(line, "[TABLE_END]") != NULL) {
                in_table_schema = 0;
                found_schema = 0;
                continue;
            }
            if (in_table_schema && strstr(line, table_name) != NULL) {
                found_schema = 1;
                continue;
            }
            if (found_schema && strstr(line, "# Columns: ") != NULL) {
                schema_data = concat(schema_data, line);
                schema_parsed = 1;  // Mark schema as parsed to avoid reprocessing
                continue;
            }
        }

        // Only process the values for the specific table
        if (strstr(line, "[TABLE_VALUE_BEGIN]") != NULL) {
            in_table_values = 1;
            continue;
        }
        if (strstr(line, "[TABLE_VALUE_END]") != NULL) {
            in_table_values = 0;
            found_table = 0;
            continue;
        }
        if (in_table_values && strstr(line, table_name) != NULL) {
            found_table = 1;
            continue;
        }
        if (found_table && strstr(line, "# Table: ") == NULL) {
            table_data = concat(table_data, line);
        }
    }

    fclose(file);

    if (strcmp(table_data, "") == 0 || strcmp(schema_data, "") == 0 || strlen(table_data) < 2) {
        free(table_data);
        free(schema_data);
        free(filename);
        return strdup("[]");  // Return an empty array
    }

    schema_data = replaceString(schema_data, "# Columns: ", "");

    // Split schema into fields
    int count = 0;
    char **result = split_string(schema_data, ",", &count);
    char *fields = calloc(1, sizeof(char));

    for (int i = 0; i < count; i++) {
        int count_ = 0;
        char **trim_result = split_string(trim(result[i]), " ", &count_);
        fields = concat(fields, trim_result[0]);
        if (i < count - 1) {
            fields = concat(fields, ",");
        }
        free(trim_result);
        free(result[i]);
    }

    free(result);
    free(schema_data);

    // Prepare to filter data based on check_field and check_value
    int filtered_count = 0;
    char *filtered_data = calloc(1, sizeof(char));
    char **rows = split_string(trim(table_data), "\n", &filtered_count);
    int check_index = -1;

    // Find the index of the check_field in the schema
    char **schema_fields = split_string(fields, ",", &count);
    for (int i = 0; i < count; i++) {
        if (strcmp(schema_fields[i], check_field) == 0) {
            check_index = i;
            break;
        }
    }

    // Filter rows based on the check_field and check_value
    for (int i = 0; i < filtered_count; i++) {
        char **data_fields = split_string(trim(rows[i]), ",", &count);
        if (check_index != -1 && strcmp(data_fields[check_index], check_value) == 0) {
            filtered_data = concat(filtered_data, rows[i]);
            strcat(filtered_data, "\n");
        }
        for (int j = 0; j < count; j++) {
            free(data_fields[j]);
        }
        free(data_fields);
    }

    free(rows);

    if (strlen(filtered_data) < 2) {
        // Free allocated memory
        free(fields);
        free(table_data);
        free(filename);
        free(filtered_data);
        free(schema_fields);
        return strdup("[]");
    }

    // Get the JSON output
    char *json_output = map_fields_to_json(fields, trim(filtered_data));

    // Free allocated memory
    free(fields);
    free(table_data);
    free(filename);
    free(filtered_data);
    free(schema_fields);

    return json_output;
}


int updateTableData(const char *database_name, const char *table_name, const char *check_field, const char *check_value, const char *update_field, const char *update_value) {
   char *update_check = fetchFilteredTableData(database_name, table_name, check_field, check_value);
   // Check for errors
   if (update_check == NULL) {
       free(update_check);
       return 0;
   }

   if(strcmp(update_check, "[]") == 0) {
	free(update_check);
	return 0;
   }

   free(update_check);

    char *filename = malloc(256);
    if (!filename) {
        perror("Memory allocation failed for filename");
        return 0;
    }

    // Construct the file path
    strcpy(filename, database_path(database_name));

    // Open the original file for reading
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        perror("Unable to open file");
        free(filename);
        return 0;
    }

    // Create a temporary file for writing
    FILE *temp_file = tmpfile();
    if (temp_file == NULL) {
        perror("Unable to create temp file");
        fclose(file);
        free(filename);
        return 0;
    }

    char line[256];
    int in_table_values = 0, found_table = 0, in_table_schema = 0, found_schema = 0;
    char *schema_data = NULL;
    int schema_field_count = 0;
    char *schema_fields = NULL;

    char updated_line[256];
    int record_found = 0, update_index = -1, check_index = -1;

    // Read the original file and write to temp file
    while (fgets(line, sizeof(line), file)) {
        // Handle [TABLE_VALUE_BEGIN] and [TABLE_VALUE_END] blocks
        if (strstr(line, "[TABLE_VALUE_BEGIN]") != NULL) {
            in_table_values = 1;
            fputs(line, temp_file);
            continue;
        }
        if (strstr(line, "[TABLE_VALUE_END]") != NULL) {
            in_table_values = 0;
            fputs(line, temp_file);
            continue;
        }

        // Handle [TABLE_BEGIN] and [TABLE_END] blocks
        if (strstr(line, "[TABLE_BEGIN]") != NULL) {
            in_table_schema = 1;
            found_schema = 0;
            fputs(line, temp_file);
            continue;
        }
        if (strstr(line, "[TABLE_END]") != NULL) {
            in_table_schema = 0;
            found_schema = 0;
            fputs(line, temp_file);
            continue;
        }

        // Table Schema Section: extract schema and avoid duplicating lines
        if (in_table_schema) {
            if (strstr(line, table_name) != NULL && !found_schema) {
                found_schema = 1;
                fputs(line, temp_file);
                continue;
            }

            if (found_schema && strstr(line, "# Columns: ") != NULL) {
                schema_data = strdup(line);
                trim_newlines(schema_data);
                schema_data = replaceString(schema_data, "# Columns: ", "");

                char **result = split_string(schema_data, ",", &schema_field_count);
                // Build schema field names
                schema_fields = malloc(256); // Ensure sufficient space
                schema_fields[0] = '\0'; // Initialize to empty string

                for (int i = 0; i < schema_field_count; i++) {
                    int count_ = 0;
                    char **trim_result = split_string(trim(result[i]), " ", &count_);
                    strcat(schema_fields, trim_result[0]);
                    if (i < schema_field_count - 1) {
                        strcat(schema_fields, ",");
                    }
                    free(trim_result);
                    free(result[i]);
                }
                free(result);
                free(schema_data);

                fputs(line, temp_file); // Write the schema line only once
                continue;
            }
        }

        // Table values section
        if (in_table_values) {
            if (strstr(line, table_name) != NULL) {
                found_table = 1;
                fputs(line, temp_file);
                continue;
            }

            if (found_table) {
                int field_count = 0;
                char **fields = split_string(trim(line), ",", &field_count);

                // Find the schema index for check and update fields
                if (check_index == -1 || update_index == -1) {
                    char **schema = split_string(trim(schema_fields), ",", &field_count);
                    for (int i = 0; i < field_count; i++) {
                        if (strcmp(schema[i], check_field) == 0) {
                            check_index = i;
                        }
                        if (strcmp(schema[i], update_field) == 0) {
                            update_index = i;
                        }
                    }
                    free(schema);
                }

                // If check_field matches, update the value
                if (strcmp(fields[check_index], check_value) == 0) {
                    record_found = 1;
                    free(fields[update_index]); // Free the old value before updating
                    fields[update_index] = strdup(update_value); // Allocate new memory

                    // Rebuild the updated line
                    strcpy(updated_line, "");
                    for (int i = 0; i < field_count; i++) {
                        strcat(updated_line, fields[i]);
                        if (i < field_count - 1) {
                            strcat(updated_line, ",");
                        }
                    }
                    trim_newlines(updated_line);
                    strcat(updated_line, "\n");

                    fputs(updated_line, temp_file);  // Write updated line to temp file
                } else {
                    // Write original line if no match
                    fputs(line, temp_file);
                }

                // Free memory for fields
                for (int i = 0; i < field_count; i++) {
                    free(fields[i]);
                }
                free(fields);
            }
        } else {
            // Copy any other non-relevant lines
            fputs(line, temp_file);
        }
    }

    // Close the original file
    fclose(file);

    // Replace the original file with the temp file
    if (record_found) {
        // Open the original file for writing
        file = fopen(filename, "w");
        if (file == NULL) {
            perror("Unable to open original file for writing");
            fclose(temp_file);
            free(filename);
            return 0;
        }

        // Rewind temp file to beginning and copy its content to the original file
        rewind(temp_file);
        while (fgets(line, sizeof(line), temp_file)) {
            fputs(line, file);
        }
    }

    // Close temp file and original file
    fclose(temp_file);
    fclose(file);
    free(filename);
    free(schema_fields); // Free schema_fields if allocated

    return record_found;
}

int deleteTableData(const char *database_name, const char *table_name, const char *check_field, const char *check_value) {
    char *filename = malloc(256);
    if (!filename) {
        perror("Memory allocation failed for filename");
        return 0;
    }

    // Construct the file path
    strcpy(filename, database_path(database_name));

    // Open the original file for reading
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        perror("Unable to open file");
        free(filename);
        return 0;
    }

    // Create a temporary file for writing
    FILE *temp_file = tmpfile();
    if (temp_file == NULL) {
        perror("Unable to create temp file");
        fclose(file);
        free(filename);
        return 0;
    }

    // Buffers and variables for schema and value processing
    char line[256];
    int in_table_values = 0, found_table = 0, in_table_schema = 0, found_schema = 0;
    char *schema_data = "";
    int schema_field_count = 0;
    char *schema_fields = "";
    int record_found = 0, check_index = -1;

    // Read the original file and write to temp file
    while (fgets(line, sizeof(line), file)) {
        // Handle [TABLE_VALUE_BEGIN] and [TABLE_VALUE_END] blocks
        if (strstr(line, "[TABLE_VALUE_BEGIN]") != NULL) {
            in_table_values = 1;
            fputs(line, temp_file);
            continue;
        }
        if (strstr(line, "[TABLE_VALUE_END]") != NULL) {
            in_table_values = 0;
            fputs(line, temp_file);
            continue;
        }

        // Handle [TABLE_BEGIN] and [TABLE_END] blocks
        if (strstr(line, "[TABLE_BEGIN]") != NULL) {
            in_table_schema = 1;
            found_schema = 0;  // Reset flag for each table
            fputs(line, temp_file); // Write [TABLE_BEGIN] line
            continue;
        }
        if (strstr(line, "[TABLE_END]") != NULL) {
            in_table_schema = 0;
            found_schema = 0;
            fputs(line, temp_file); // Write [TABLE_END] line
            continue;
        }

        // Table Schema Section: extract schema
        if (in_table_schema) {
            if (strstr(line, table_name) != NULL && !found_schema) {
                found_schema = 1;  // Found the target table
                fputs(line, temp_file);  // Write the table name line only once
                continue;
            }

            // Process schema if we found the correct table
            if (found_schema && strstr(line, "# Columns: ") != NULL) {
                // Parse and save the schema data
                schema_data = strdup(line);
                trim_newlines(schema_data);
                schema_data = replaceString(schema_data, "# Columns: ", "");

                char **result = split_string(schema_data, ",", &schema_field_count);
                // Build schema field names
                for (int i = 0; i < schema_field_count; i++) {
                    int count_ = 0;
                    char **trim_result = split_string(trim(result[i]), " ", &count_);
                    schema_fields = concat(schema_fields, trim_result[0]);
                    if (i < schema_field_count - 1) {
                        schema_fields = concat(schema_fields, ",");
                    }
                    free(trim_result);
                    free(result[i]);
                }
                free(result);
                free(schema_data);

                fputs(line, temp_file);  // Write the schema line only once
                continue;
            }
        }

        // Table values section
        if (in_table_values) {
            if (strstr(line, table_name) != NULL) {
                found_table = 1;
                fputs(line, temp_file);  // Copy table name line
                continue;
            }

            if (found_table) {
                int field_count = 0;
                char **fields = split_string(trim(line), ",", &field_count);

                // Find the schema index for the check field
                if (check_index == -1) {
                    char **schema = split_string(trim(schema_fields), ",", &field_count);
                    for (int i = 0; i < field_count; i++) {
                        if (strcmp(schema[i], check_field) == 0) {
                            check_index = i;
                        }
                    }
                    free(schema);
                }

                // Check if the line matches the deletion criteria
                if (check_index != -1 && strcmp(fields[check_index], check_value) == 0) {
                    record_found = 1; // Record to be deleted found
                } else {
                    // Write the original line if it does not match
                    fputs(line, temp_file);
                }

                // Free memory for fields
                for (int i = 0; i < field_count; i++) {
                    free(fields[i]);
                }
                free(fields);
            }
        } else {
            // Copy any other non-relevant lines
            fputs(line, temp_file);
        }
    }

    // Close the original file
    fclose(file);

    // If no records were found, free resources and return 0
    if (!record_found) {
        fclose(temp_file);
        free(schema_fields);
        free(filename);
        return 0;
    }

    // Replace the original file with the temp file if any records were deleted
    // Open the original file for writing
    file = fopen(filename, "w");
    if (file == NULL) {
        perror("Unable to open original file for writing");
        fclose(temp_file);
        free(filename);
        return 0;
    }

    // Rewind temp file to beginning and copy its content to the original file
    rewind(temp_file);
    while (fgets(line, sizeof(line), temp_file)) {
        fputs(line, file);
    }

    // Close temp file and original file
    fclose(temp_file);
    fclose(file);
    free(schema_fields);
    free(filename);

    return 1; // Return 1 if record was found and deleted
}

int deleteTable(const char *database_name, const char *table_name) {
    char *filename = malloc(256);
    if (!filename) {
        perror("Memory allocation failed");
        return 0; // Indicate failure
    }

    // Construct the file path
    strcpy(filename, database_path(database_name)); // Example path

    // Open the original file for reading
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        perror("Unable to open file");
        free(filename);
        return 0; // Indicate failure
    }

    // Create a temporary file for writing
    FILE *temp_file = tmpfile();
    if (temp_file == NULL) {
        perror("Unable to create temp file");
        fclose(file);
        free(filename);
        return 0; // Indicate failure
    }

    char line[256];
    int in_table_values = 0;
    int found_table = 0;
    int skip_schema = 0;
    int skip_table_values = 0;
    int after_columns = 0; // New flag to track if we are after the # Columns line

    // Read the original file and write to the temp file
    while (fgets(line, sizeof(line), file)) {

        // Check for the beginning of the values section
        if (strstr(line, "[TABLE_VALUE_BEGIN]") != NULL) {
            in_table_values = 1; // Start of values section
            fputs(line, temp_file); // Write the section header
            continue;
        }

        // Check for the end of the values section
        if (strstr(line, "[TABLE_VALUE_END]") != NULL) {
            in_table_values = 0; // End of values section
            fputs(line, temp_file); // Write the section footer
            continue;
        }

        // Handle table schema deletion in the schema section
        if (!in_table_values && strstr(line, "# Table:") != NULL) {
            // Check if the current table is the one to delete
            if (strstr(line, table_name) != NULL) {
                found_table = 1;   // Found the table to delete
                skip_schema = 1;   // Start skipping schema
                after_columns = 0;  // Reset after_columns
                continue;          // Skip the "# Table:" line
            } else {
                skip_schema = 0;   // Stop skipping if it's not the table
            }
        }

        // Skip schema lines for the found table
        if (skip_schema) {
            // Skip the "# Columns:" line
            if (strstr(line, "# Columns:") != NULL) {
                continue; // Skip the "# Columns:" line
            }

            // Check for an empty line to set after_columns flag
            if (strlen(line) == 1) { // Detect empty line (just newline character)
                after_columns = 1;   // Mark that we are after the # Columns line
                continue; // Skip the empty line
            }

            // Skip schema lines that contain exactly two words, but only if we're after # Columns line
            if (after_columns && countWords(line) == 2) {
                continue; // Skip lines like "id INTEGER" or "username TEXT"
            }
        }

        // Handle table values deletion in the values section
        if (in_table_values && strstr(line, table_name) != NULL) {
            skip_table_values = 1; // Start skipping table values
            continue;
        }

        // Stop skipping values when we find another table or reach the end of the block
        if (in_table_values && strstr(line, "# Table:") != NULL && skip_table_values) {
            skip_table_values = 0;
        }

        // Skip table values if we are inside the values block for the table to delete
        if (skip_table_values) {
            continue;
        }

        // Write all other lines that are not related to the deleted table
        fputs(line, temp_file);
    }

    // Close files
    fclose(file);

    // Replace the original file with the temp file if the table was found
    if (found_table) {
        // Open the original file for writing
        file = fopen(filename, "w");
        if (file == NULL) {
            perror("Unable to open original file for writing");
            fclose(temp_file);
            free(filename);
            return 0; // Indicate failure
        }

        // Rewind temp file to beginning and copy its content to the original file
        rewind(temp_file);
        while (fgets(line, sizeof(line), temp_file)) {
            fputs(line, file);
        }

        fclose(file);
    } else {
	found_table = 0;
    }

    // Close the temp file
    fclose(temp_file);
    free(filename);
    if(found_table == 0){
      return found_table;
    } else {
    remove_extra_empty_lines(filename);
    return found_table; // Return whether the table was found and deleted
    }

}

char *listTable(const char* database_name) {
    char filename[256];
    strcpy(filename, database_path(database_name));  // Ensure database_path returns a valid path
    FILE* file = fopen(filename, "r");
    if (file == NULL) {
        perror("Unable to open file");
        return NULL;
    }

    char line[256];
    int in_table_block = 0;
    char* tables[1000];
    int table_count = 0;

    while (fgets(line, sizeof(line), file)) {
        char* trimmed_line = trim(line);  // Ensure trim is working correctly

        // Debug to check the current line and if we are inside the table block

        if (strstr(line, "[TABLE_BEGIN]") != NULL) {
            in_table_block = 1;
	  } else if (strstr(line, "[TABLE_END]") != NULL) {
            in_table_block = 0;
            break;  // You may want to remove this if there are multiple tables
        }

	trim_newlines(trimmed_line);

        // Check if we are inside the table block and if the line contains the table name
        if (in_table_block && strstr(trimmed_line, "# Table:") == trimmed_line) {
            // Extract table name after "# Table: "
            char* table_name = trimmed_line + strlen("# Table:");
            table_name = trim(table_name);  // Ensure proper trimming of the table name
	    tables[table_count] = strdup(table_name);  // Dynamically allocate memory for the table name
	    table_count++;
        }
    }

    fclose(file);

    // Create a JSON string to return the table names
    char* json_result = (char*)malloc(1024);  // Ensure the buffer size is enough
    if (json_result == NULL) {
        perror("Unable to allocate memory");
        return NULL;
    }
    strcpy(json_result, "{ \"tables\": [");

    for (int i = 0; i < table_count; i++) {
        strcat(json_result, "\"");
        strcat(json_result, tables[i]);
        strcat(json_result, "\"");
        if (i < table_count - 1) {
            strcat(json_result, ",");
        }
        free(tables[i]);  // Free the dynamically allocated memory
    }

    strcat(json_result, "] }");

    if (table_count == 0) {
        strcpy(json_result, "{ \"tables\": [] }");
    }

    return json_result;
}
