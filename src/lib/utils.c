#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include "constants.c"

void writeToFile(const char *filename, const char *data) {
    FILE *file = fopen(filename, "w"); // Open file for writing
    if (file == NULL) {
        perror("Unable to open file for writing");
        return;
    }
    fprintf(file, "%s", data); // Write data to the file
    fclose(file); // Close the file
}

// Function to append schema to a file
void appendToFile(const char *filename, const char *data) {
    // Open the file in append mode
    FILE *file = fopen(filename, "a");
    if (file == NULL) {
        perror("Failed to open file");
        return;
    }

    // Write the data to the file
    fputs(data, file);

    // Close the file
    fclose(file);
}


void sanitize_str(const char *input, char *output, size_t output_size, char *extension) {
    size_t j = 0; 
    for (size_t i = 0; input[i] != '\0' && j < output_size - 1; i++) {
        char ch = input[i];
        ch = tolower((unsigned char)ch);
        if (ch == ' ') {
            ch = '_';
        }
        if (isalnum(ch) || ch == '_') {
            output[j++] = ch; 
        }
    }
    output[j] = '\0'; 
    if(extension == NULL) {
	extension = "";    
    };
    if (j >= 3 && strcmp(&output[j - 3], extension) == 0) {
        output[j - 3] = '\0'; 
    }
}

int create_directory(const char *dir_name) {
    struct stat st = {0};

    // Check if the directory already exists
    if (stat(dir_name, &st) == -1) {
        // Directory does not exist, so create it
        if (mkdir(dir_name, 0700) == -1) {
            perror("mkdir failed");
            return -1; // Return -1 on failure
        }
    } 
    return 0; // Return 0 on success
}

char* concat(const char* str1, const char* str2){
    char* result;
    asprintf(&result, "%s%s", str1, str2);
    return result;
}

// Function to trim leading and trailing whitespaces
char* trim(char* str) {
    char* end;

    // Trim leading space
    while(*str == ' ') str++;

    // If all spaces
    if(*str == 0)
        return str;

    // Trim trailing space
    end = str + strlen(str) - 1;
    while(end > str && *end == ' ') end--;

    // Write new null terminator character
    *(end+1) = 0;

    return str;
}

void trim_newlines(char* str) {
    int len = strlen(str);

    // Remove trailing newlines
    while (len > 0 && (str[len - 1] == '\n')) {
        str[len - 1] = '\0';
        len--;
    }

    // Remove leading newlines
    int start = 0;
    while (str[start] == '\n') {
        start++;
    }

    // Shift the string if leading newlines were found
    if (start > 0) {
        memmove(str, str + start, len - start + 1);
    }
}

char *replaceString(const char *str, const char *oldWord, const char *newWord) {
    char *result;
    int i, count = 0;
    int newWordLen = strlen(newWord);
    int oldWordLen = strlen(oldWord);

    // Count the number of times the old word appears in the string
    for (i = 0; str[i] != '\0'; i++) {
        if (strstr(&str[i], oldWord) == &str[i]) {
            count++;
            i += oldWordLen - 1;
        }
    }

    // Allocate memory for the new string
    result = (char *)malloc(i + count * (newWordLen - oldWordLen) + 1);

    if (result == NULL) {
        printf("Memory allocation failed.\n");
        return NULL;
    }

    i = 0;
    while (*str) {
        // Compare the substring with the result
        if (strstr(str, oldWord) == str) {
            strcpy(&result[i], newWord);
            i += newWordLen;
            str += oldWordLen;
        } else {
            result[i++] = *str++;
        }
    }

    result[i] = '\0';
    return result;
}

char **split_string(const char* str, const char* delimiter, int* count) {
    // Make a copy of the input string since strtok modifies the string
    char* str_copy = strdup(str);

    // Count how many tokens we will get
    *count = 0;
    char* temp = strdup(str);
    char* token = strtok(temp, delimiter);
    while (token != NULL) {
        (*count)++;
        token = strtok(NULL, delimiter);
    }
    free(temp);

    // Allocate memory for storing the tokens
    char** tokens = (char**)malloc((*count) * sizeof(char*));
    if (tokens == NULL) {
        free(str_copy);
        return NULL; // Memory allocation failed
    }

    // Split the string and store the tokens
    int index = 0;
    token = strtok(str_copy, delimiter);
    while (token != NULL) {
        tokens[index++] = strdup(token); // Duplicate token for safe storage
        token = strtok(NULL, delimiter);
    }

    free(str_copy); // Free the copy of the original string

    return tokens;
}

// Function to map fields to values and return JSON
char* map_fields_to_json(const char* fields, const char* values) {
    int fieldCount = 0, lineCount = 0;

    // Split fields
    char** fieldTokens = split_string(trim(fields), ",", &fieldCount);
    // Split values by newline to get multiple lines
    char** valueTokens = split_string(trim(values), "\n", &lineCount);

    // Check if the first line has the same number of values as fields
    int countMatcher = 0;
    split_string(valueTokens[0], ",", &countMatcher);

    // Check if field and value counts match
    if (fieldCount != countMatcher) {
        printf("Field and value counts do not match.\n");
        // Free allocated memory
        for (int i = 0; i < fieldCount; i++) {
            free(fieldTokens[i]);
        }
        free(fieldTokens);
        free(valueTokens);
        return NULL;
    }

    // Start building the JSON string
    char* jsonResult = strdup("[");

    for (int i = 0; i < lineCount; i++) {
        int valueCount = 0;
        char** lineValue = split_string(trim(valueTokens[i]), ",", &valueCount);

        // Check if the lineValue count matches fieldCount
        if (valueCount != fieldCount) {
            printf("Field count and value count do not match for line %d.\n", i);
            for (int j = 0; j < valueCount; j++) {
                free(lineValue[j]);
            }
            free(lineValue);
            continue; // Skip this line if it doesn't match
        }

        // Start building the JSON object for the current line
        char* jsonLine = strdup("{");
        for (int j = 0; j < fieldCount; j++) {
            // Create JSON key-value pair
            char* key = trim(fieldTokens[j]);
            char* value = trim(lineValue[j]);

            // Append key and value to JSON line
            char* jsonPair = concat(concat("\"", key), "\":\"");
            jsonPair = concat(jsonPair, value);
            jsonPair = concat(jsonPair, "\"");

            // Append to result JSON line
            jsonLine = concat(concat(jsonLine, jsonPair), (j == fieldCount - 1) ? "" : ",");

            // Free temporary JSON pair
            free(jsonPair);
        }

        // Close JSON object
        jsonLine = concat(jsonLine, "}");

        // Append the JSON object to the result
        jsonResult = concat(concat(jsonResult, jsonLine), (i == lineCount - 1) ? "" : ",");

        // Free allocated memory for the line JSON
        free(jsonLine);

        // Free line values
        for (int j = 0; j < valueCount; j++) {
            free(lineValue[j]);
        }
        free(lineValue);
    }
    // Close JSON array
    jsonResult = concat(jsonResult, "]");

    // Free allocated memory for tokens
    for (int i = 0; i < fieldCount; i++) {
        free(fieldTokens[i]);
    }
    free(fieldTokens);

    for (int i = 0; i < lineCount; i++) {
        free(valueTokens[i]);
    }
    free(valueTokens);

    return jsonResult;
}


char* database_path(char* filename) {
    char* path = concat(DB_DIRECTORY, "/");
    path = concat(path, filename);
    path = concat(path, ".db");
    return path;
}

// Function to count the number of words in a line
int countWords(const char *line) {
    int count = 0;
    int inWord = 0;
    
    for (int i = 0; line[i] != '\0'; i++) {
        if (isspace(line[i])) {
            inWord = 0;
        } else if (!inWord) {
            inWord = 1;
            count++;
        }
    }
    
    return count;
}

// Function to check if a line is empty (only contains whitespaces or is a newline)
bool is_empty_line(const char* line) {
    for (int i = 0; line[i] != '\0'; i++) {
        if (line[i] != '\n' && line[i] != '\r' && line[i] != ' ' && line[i] != '\t') {
            return false;
        }
    }
    return true;
}

void remove_extra_empty_lines(const char* filepath) {
    FILE *input_file = fopen(filepath, "r");
    if (input_file == NULL) {
        perror("Unable to open file");
        return;
    }

    // Create a temporary file to store the modified content
    FILE *temp_file = tmpfile();
    if (temp_file == NULL) {
        perror("Unable to create temporary file");
        fclose(input_file);
        return;
    }

    char line[256];
    int empty_line_count = 0;  // Tracks consecutive empty lines

    while (fgets(line, sizeof(line), input_file)) {
        if (is_empty_line(line)) {
            empty_line_count++;
        } else {
            empty_line_count = 0;  // Reset count if non-empty line is found
        }

        // Write the line only if we haven't exceeded 3 consecutive empty lines
        if (empty_line_count <= 2) {
            fputs(line, temp_file);
        }
    }

    fclose(input_file);

    // Now overwrite the original file with the content from the temp file
    input_file = fopen(filepath, "w");
    if (input_file == NULL) {
        perror("Unable to reopen file for writing");
        fclose(temp_file);
        return;
    }

    // Rewind the temp file and write its content to the original file
    rewind(temp_file);
    while (fgets(line, sizeof(line), temp_file)) {
        fputs(line, input_file);
    }

    fclose(temp_file);
    fclose(input_file);

}

void initialize(){
    // Create a directory to store the database
    const char *directory_name = DB_DIRECTORY;
    //check for the config file
    if(access("config", F_OK) != 0) {
      printf("=> Config file not found, creating one...\n");
      writeToFile("config", DEFAULT_CONFIG);
    }
    // Create the directory
    if (create_directory(directory_name) == -1) {
	perror("Failed to create directory");
	exit(1);
    }
}

// Function to extract a value from a JSON string by key
char *extract_json_value(const char *json, const char *key) {
    char *start = NULL;
    char *end = NULL;
    char *value = NULL;

    // Create a key string to search for (including the quotes and colon)
    char search_key[256];
    snprintf(search_key, sizeof(search_key), "\"%s\"", key);

    // Find the key in the JSON string
    start = strstr(json, search_key);
    if (!start) {
        return NULL; // Key not found
    }

    // Move to the colon after the key
    start = strchr(start, ':');
    if (!start) {
        return NULL; // Malformed JSON (no colon found)
    }
    start++; // Move past the colon

    // Skip any whitespace characters
    while (*start == ' ' || *start == '\t' || *start == '\n') {
        start++;
    }

    // Check if the value is a string (starts with a quote)
    if (*start == '\"') {
        start++; // Skip the opening quote
        end = strchr(start, '\"'); // Find the closing quote
        if (!end) {
            return NULL; // No closing quote found
        }
    } else {
        // If not a string, find the next comma or closing brace
        end = strpbrk(start, ",}");
        if (!end) {
            return NULL; // No valid delimiter found
        }
    }

    // Allocate memory for the extracted value
    size_t value_length = end - start;
    value = (char *)malloc(value_length + 1);
    if (!value) {
        return NULL; // Memory allocation failed
    }

    // Copy the value into the allocated memory
    strncpy(value, start, value_length);
    value[value_length] = '\0'; // Null-terminate the string

    return value; // Return the extracted value
}
