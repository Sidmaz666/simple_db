#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/socket.h>  // Include this for the 'recv' function
#include <arpa/inet.h>
#include "../lib/constants.c"

#define SUCCESS "200 OK"
#define UNAUTHORIZED "401 Unauthorized"
#define BAD_REQUEST "400 Bad Request"
#define NOT_FOUND "404 Not Found"


extern char *listDB(const char *directory);
extern char *listTable(const char *database_name); 
extern char **split_string(const char *str, const char *delimiter, int *count);
extern char *extract_json_value(const char *json, const char *key);
extern int createDB(const char *database_name);
extern int createTable(const char *database_name, const char *table_name, const char *columns[], const char *types[], int column_count);
extern int insertTableValues(const char *database_name, const char *table_name, const char *values);
extern int updateTableData(const char *database_name, const char *table_name, const char *check_field, const char *check_value, const char *update_field, const char *update_value);
extern char* fetchTableData(const char *database_name, const char *table_name);
extern char* fetchFilteredTableData(const char *database_name, const char *table_name, const char *check_field, const char *check_value); 
extern int deleteDB(const char *database_name); 
extern int deleteTable(const char *database_name, const char *table_name);
extern int deleteTableData(const char *database_name, const char *table_name, const char *check_field, const char *check_value);


void send_response(int client_socket, const char *status, const char *content_type, const char *body) {
    char response[1024];
    sprintf(response, "HTTP/1.1 %s\nContent-Type: %s\nContent-Length: %lu\n\n%s", status, content_type, strlen(body), body);
    write(client_socket, response, strlen(response));
}

void get_query_value(const char *query, const char *field, char *result, size_t result_size) {
    result[0] = '\0';  // Initialize result as an empty string

    char query_copy[256];
    strncpy(query_copy, query, sizeof(query_copy) - 1);  // Make a copy of the query string
    query_copy[sizeof(query_copy) - 1] = '\0';  // Ensure null-termination

    char *token = strtok(query_copy, "&");  // Split query by '&'
    while (token != NULL) {
        // Find '=' in the current token
        char *equal_sign = strchr(token, '=');
        if (equal_sign != NULL) {
            *equal_sign = '\0';  // Split the token into field and value
            char *key = token;
            char *value = equal_sign + 1;

            // Check if the current key matches the requested field
            if (strcmp(key, field) == 0) {
                // Copy the value to the result
                strncpy(result, value, result_size - 1);
                result[result_size - 1] = '\0';  // Ensure null-termination
                return;  // Stop searching once we find the field
            }
        }
        // Move to the next key-value pair
        token = strtok(NULL, "&");
    }
    // If field is not found, result will be an empty string
}

void handle_request(char *request, int client_socket, char *username, char *password) {
    char body[1024];  // Buffer to store the body content for POST requests
    char path[256];  // Buffer to store the request path (without query parameters)
    char query[256]; // Buffer to store the query string (if any)
    char content_type[256] = "unknown";  // Default content-type
    int content_length = 0;  // Store the content length for the body
    int bytes_read = 0; // Track how much of the body is read

    // Initialize the query and body to empty strings
    query[0] = '\0';
    body[0] = '\0';

    //printf("Received request - %s\n", request);

    // Parse request line
    char *method = strtok(request, " ");
    char *full_path = strtok(NULL, " ");
    char *http_version = strtok(NULL, "\r\n");

    printf("=> (%s) %s [%s]\n", method, full_path, http_version);

    // Check if the request path is valid
    if (full_path == NULL) {
        printf("Invalid path\n");
        send_response(
	    client_socket, 
	    NOT_FOUND, 
	    "application/json", "{ \"status\": \"404 Not Found\", \"response\": null, \"message\": \"Path not found.\" }");
        return;
    }

    // Parse the query string, if present
    char *query_start = strchr(full_path, '?');
    if (query_start != NULL) {
        size_t path_length = query_start - full_path;
        strncpy(path, full_path, path_length);
        path[path_length] = '\0';
        strncpy(query, query_start + 1, sizeof(query) - 1);
        query[sizeof(query) - 1] = '\0';
    } else {
        strncpy(path, full_path, sizeof(path) - 1);
        path[sizeof(path) - 1] = '\0';
    }

    // Parse headers
    char *header = strtok(NULL, "\r\n");
    while (header != NULL && strlen(header) > 0) {
        if (strncmp(header, "Content-Length:", 15) == 0) {
            content_length = atoi(header + 16);
        } else if (strncmp(header, "Content-Type:", 13) == 0) {
            strncpy(content_type, header + 14, sizeof(content_type) - 1);
            content_type[sizeof(content_type) - 1] = '\0';
        }

        header = strtok(NULL, "\r\n");
    }

    // Check for POST method and read the body
    if (strcmp(method, "POST") == 0 && content_length > 0) {
        int total_bytes_read = 0;
        char buffer[1024];

        while (total_bytes_read < content_length) {
            bytes_read = recv(client_socket, buffer, sizeof(buffer) - 1, 0);
            if (bytes_read <= 0) {
                printf("Error reading body\n");
                break;
            }
            buffer[bytes_read] = '\0';
            strncat(body, buffer, sizeof(body) - strlen(body) - 1);  // Concatenate the data to the body buffer
            total_bytes_read += bytes_read;
        }

	char *json_start = strchr(body, '{');
	// Setting the json_start value to the body
	if (json_start != NULL) {
	      strncpy(body, json_start, sizeof(body) - 1);  // Copy the JSON to body
	      body[sizeof(body) - 1] = '\0';  // Ensure null termination
	    } else {
	      strcpy(body, "No JSON found");  // Handle the case where no JSON is found
	  }
	printf("=> POST Data = %s\n", body);
    }

    // Authentication logic
    char username_from_req[256];
    char password_from_req[256];

    get_query_value(query, "username", username_from_req, sizeof(username_from_req));
    get_query_value(query, "password", password_from_req, sizeof(password_from_req));

    if (strcmp(username_from_req, username) != 0 || strcmp(password_from_req, password) != 0) {
        send_response(client_socket, UNAUTHORIZED, "application/json", "{\"status\": \"0\",\"response\": \"Unauthorized\"}");
        return;
    }

    // Handle different paths
    if (strcmp(path, "/list/db") == 0 && strcmp(method, "GET") == 0) {
        char *database_list = listDB(DB_DIRECTORY);
        char response_body[600];
        sprintf(response_body, "{\"status\": \"%s\", \"response\": %s}", SUCCESS, database_list);
        send_response(client_socket, SUCCESS, "application/json", response_body);
        free(database_list);
    } else if (strncmp(path, "/list/table/data/", 16) == 0 && strcmp(method, "GET") == 0) {
        char data[256];
        char response_body[1024];
        sscanf(path + 16, "%s", data);
	int data_count = 0;
	char **data_array = split_string(data, "/", &data_count);
	char *database_name = data_array[0];
	char *table_name = data_array[1];
	if(database_name == NULL || table_name == NULL) {
            sprintf(response_body, "{ \"status\": \"%s\", \"response\": []}", BAD_REQUEST);
            send_response(client_socket, BAD_REQUEST, "application/json", response_body);
	      for(int i = 0; i < data_count; i++) {
		  free(data_array[i]);
	      }
	    free(data_array);
	    return;
	}
        char *result = fetchTableData(database_name,table_name);
        if (result) {
            sprintf(
		response_body, 
		"{ \"status\": \"%s\", \"response\": %s, \"database\": \"%s\" , \"table\": \"%s\"}", 
		SUCCESS, 
		result, 
		database_name,
		table_name
		);
            send_response(client_socket, SUCCESS, "application/json", response_body);
        } else {
            sprintf(
		response_body, 
		"{ \"status\": \"%s\", \"response\": [], \"database\": \"%s\", \"table\": \"%s\" }", 
		SUCCESS,
		database_name,
		table_name
		);
            send_response(client_socket, SUCCESS, "application/json", response_body);
        }
	  for(int i = 0; i < data_count; i++) {
	      free(data_array[i]);
	  }
	free(data_array);
        free(result);
    } else if (strncmp(path, "/list/table/filter/", 18) == 0 && strcmp(method, "GET") == 0) {
        char data[256];
        char response_body[1024];
        sscanf(path + 18, "%s", data);
	int data_count = 0;
	char **data_array = split_string(data, "/", &data_count);
	char *database_name = data_array[0];
	char *table_name = data_array[1];
	char *check_field = data_array[2];
	char *check_value = data_array[3];
	if(database_name == NULL || table_name == NULL || check_field == NULL || check_value == NULL) {
            sprintf(response_body, "{ \"status\": \"%s\", \"response\": []}", BAD_REQUEST);
            send_response(client_socket, BAD_REQUEST, "application/json", response_body);
	      for(int i = 0; i < data_count; i++) {
		  free(data_array[i]);
	      }
	    free(data_array);
	    return;
	}
	char *result = fetchFilteredTableData(database_name, table_name, check_field, check_value);
        if (result != NULL) {
            sprintf(
		response_body, 
		"{ \"status\": \"%s\", \"response\": %s, \"database\": \"%s\" , \"table\": \"%s\"}", 
		SUCCESS, 
		result, 
		database_name,
		table_name
		);
            send_response(client_socket, SUCCESS, "application/json", response_body);
        } else {
            sprintf(
		response_body, 
		"{ \"status\": \"%s\", \"response\": [], \"database\": \"%s\", \"table\": \"%s\" }", 
		SUCCESS,
		database_name,
		table_name
		);
            send_response(client_socket, SUCCESS, "application/json", response_body);
        }
	  for(int i = 0; i < data_count; i++) {
	      free(data_array[i]);
	  }
	free(data_array);
        free(result);
    }   else if (strncmp(path, "/list/table/", 12) == 0 && strcmp(method, "GET") == 0) {
        char database_name[256];
        sscanf(path + 12, "%s", database_name);
        char response_body[600];
        char *tresult = listTable(database_name);
        if (tresult) {
            sprintf(response_body, "{ \"status\": \"%s\", \"response\": %s, \"database\": \"%s\"}", SUCCESS, tresult, database_name);
            send_response(client_socket, SUCCESS, "application/json", response_body);
        } else {
            sprintf(response_body, "{ \"status\": \"%s\", \"response\": {\"tables\": []}}", SUCCESS);
            send_response(client_socket, SUCCESS, "application/json", response_body);
        }
        free(tresult);
    } else if (strcmp(path, "/create/db") == 0 && strcmp(method, "POST") == 0) {
        char response_body[600];
	char *database_name = extract_json_value(body, "database_name");
	if(database_name == NULL && strlen(database_name) <= 1) {
		sprintf(response_body, "{\"status\": %s, \"data\": %s}", BAD_REQUEST, body);
		send_response(client_socket, BAD_REQUEST, "application/json", response_body);
		return;
	}
        int db_create_result = createDB(database_name);
	// // Check the result and print appropriate message
        if (db_create_result > 0) {
	    sprintf(
		response_body, 
		"{\"status\": %s, \"response\": %s , \"message\": \"Database '%s' created successfully.\"}", 
		SUCCESS,
		body,
		database_name
		);
	    send_response(client_socket, SUCCESS , "application/json", response_body);
        } else {
	    sprintf(
		response_body, 
		"{\"status\": \"203 Conflict\", \"response\": %s, \"message\": \"Database '%s' already exists.\"}", 
		body,
		database_name
		);
	    send_response(client_socket, "203 Conflict", "application/json", response_body);
        }
	free(database_name);
    } else if (strcmp(path, "/create/table") == 0 && strcmp(method, "POST") == 0) {
        char response_body[600];
	char *table_name = extract_json_value(body, "table_name");
	char *database_name = extract_json_value(body, "database_name");
	int column_count = 0;
	int type_count = 0;
	char **columns = split_string(extract_json_value(body, "columns"), ",", &column_count);
	char **types =  split_string(extract_json_value(body, "types"), ",", &type_count);
        if (
	    table_name == NULL || strlen(table_name) == 0 || 
	    database_name == NULL || strlen(database_name) == 0 ||
	    column_count == 0 || type_count == 0 ||
	    column_count != type_count || 
	    columns == NULL || types == NULL
	    ) {
		sprintf(response_body, "{\"status\": %s, \"data\": %s}", BAD_REQUEST, body);
		send_response(client_socket, BAD_REQUEST, "application/json", response_body);
		free(table_name);
		free(database_name);
		free(columns);
		free(types);
		return;
	}
	int db_table_create_result = createTable(
	      database_name, 
	      table_name, 
	      (const char **)columns, 
	      (const char **)types, 
	      column_count
	      );
 	 if(db_table_create_result > 0) {
	    sprintf(
		response_body, 
		"{\"status\": %s, \"response\": %s , \"message\": \"Database '%s' created successfully.\"}", 
		SUCCESS,
		body,
		table_name
		);
	    send_response(client_socket, SUCCESS , "application/json", response_body);
        } else {
	    sprintf(
		response_body, 
		"{\"status\": \"203 Conflict\", \"response\": %s, \"message\": \"Database '%s' already exists.\"}", 
		body,
		table_name
		);
	    send_response(client_socket, "203 Conflict", "application/json", response_body);
        }
	free(table_name);
	free(database_name);
	free(columns);
	free(types);
    } else if (strcmp(path, "/insert") == 0 && strcmp(method, "POST") == 0) {
        char response_body[600];
	char *table_name = extract_json_value(body, "table_name");
	char *database_name = extract_json_value(body, "database_name");
	char *value = extract_json_value(body, "value");
        if (
	    table_name == NULL || strlen(table_name) == 0 || 
	    database_name == NULL || strlen(database_name) == 0 ||
	    value == NULL || strlen(value) == 0
	    ) {
		sprintf(response_body, "{\"status\": %s, \"data\": %s}", BAD_REQUEST, body);
		send_response(client_socket, BAD_REQUEST, "application/json", response_body);
		free(table_name);
		free(database_name);
		free(value);
		return;
	}
	 int insert_result = insertTableValues(database_name, table_name, value);
 	 if(insert_result > 0) {
	    sprintf(
		response_body, 
		"{\"status\": %s, \"response\": %s , \"message\": \"Values in Table: '%s' inserted successfully.\"}", 
		SUCCESS,
		body,
		table_name
		);
	    send_response(client_socket, SUCCESS , "application/json", response_body);
        } else {
	    sprintf(
		response_body, 
		"{\"status\": \"203 Conflict\", \"response\": %s, \"message\": \"Values in Table: '%s' were not inserted.\"}", 
		body,
		table_name
		);
	    send_response(client_socket, "203 Conflict", "application/json", response_body);
        }
	free(table_name);
	free(database_name);
	free(value);
    } else if (strcmp(path, "/update") == 0 && strcmp(method, "POST") == 0) {
        char response_body[600];
	char *table_name = extract_json_value(body, "table_name");
	char *database_name = extract_json_value(body, "database_name");
	const char *check_field = extract_json_value(body, "target_field");
	const char *check_value = extract_json_value(body, "target_value");
	const char *update_field = extract_json_value(body, "new_field");
	const char *update_value = extract_json_value(body, "new_value");
        if (
	    table_name == NULL || strlen(table_name) == 0 || 
	    database_name == NULL || strlen(database_name) == 0 ||
	    check_field == NULL || strlen(check_field) == 0 ||
	    check_value == NULL || strlen(check_value) == 0 ||
	    update_field == NULL || strlen(update_field) == 0 ||
	    update_value == NULL || strlen(update_value) == 0
	    ) {
		sprintf(response_body, "{\"status\": %s, \"data\": %s}", BAD_REQUEST, body);
		send_response(client_socket, BAD_REQUEST, "application/json", response_body);
		free(table_name);
		free(database_name);
		free(check_field);
		free(check_value);
		free(update_field);
		free(update_value);
		return;
	}
		
		 int update_result = updateTableData(
		     database_name, 
		     table_name, 
		     check_field, 
		     check_value, 
		     update_field, 
		     update_value
		     );
 	 if(update_result > 0) {
	    sprintf(
		response_body, 
		"{\"status\": %s, \"response\": %s , \"message\": \"Table: '%s' , Field: '%s',  Value: '%s', Updated Field '%s', NEW_VALUE: '%s'  updated successfully.\"}", 
		SUCCESS,
		body,
		table_name,
		check_field,
		check_value,
		update_field,
		update_value
		);
	    send_response(client_socket, SUCCESS , "application/json", response_body);
        } else {
	    sprintf(
		response_body, 
		"{\"status\": %s, \"response\": %s , \"message\": \"Table: '%s' , Field: '%s',  Value: '%s', Updated Field '%s', NEW_VALUE: '%s'  update failed.\"}", 
		SUCCESS,
		body,
		table_name,
		check_field,
		check_value,
		update_field,
		update_value
		);
	    send_response(client_socket, SUCCESS , "application/json", response_body);
        }
	free(table_name);
	free(database_name);
	free(check_field);
	free(check_value);
	free(update_field);
	free(update_value);
    } else if (strncmp(path, "/delete/db/", 11) == 0 && strcmp(method, "DELETE") == 0) {
        char database_name[256];
        sscanf(path + 11, "%s", database_name);
        char response_body[600];
 	int result = deleteDB(database_name);
        if (result == 0) {
            sprintf(
		response_body, 
		"{ \"status\": \"%s\", \"response\": %d, \"database\": \"%s\", \"message\": \"%s\" }", 
		SUCCESS, 
		result, 
		database_name,
		"Database deleted successfully."
		);
            send_response(client_socket, SUCCESS, "application/json", response_body);
        } else {
            sprintf(
		response_body, 
		"{ \"status\": \"%s\", \"response\": null, \"database\": \"%s\", \"message\": \"%s\" }", 
		SUCCESS,
		database_name,
		"Unable to delete database."
		);
            send_response(client_socket, SUCCESS, "application/json", response_body);
        }
    }  else if (strncmp(path, "/delete/table/data/", 19) == 0 && strcmp(method, "DELETE") == 0) {
	char data[256];
        sscanf(path + 19, "%s", data);
	int data_count = 0;
	char **data_array = split_string(data, "/", &data_count);
	char *database_name = data_array[0];
	char *table_name = data_array[1];
	char *check_field = data_array[2];
	char *check_value = data_array[3];
        char response_body[600];
 	int result = deleteTableData(database_name, table_name, check_field, check_value);
        if (result) {
            sprintf(
		response_body, 
		"{ \"status\": \"%s\", \"response\": %d, \"database\": \"%s\", \"table\": \"%s\", \"message\": \"%s\", \"check_field\": \"%s\", \"check_value\": \"%s\" }", 
		SUCCESS, 
		result, 
		database_name,
		table_name,
		"Table Row deleted successfully.",
		check_field,
		check_value
		);
            send_response(client_socket, SUCCESS, "application/json", response_body);
        } else {
            sprintf(
		response_body, 
		"{ \"status\": \"%s\", \"response\": null, \"database\": \"%s\", \"table\": \"%s\", \"message\": \"%s\", \"check_field\": \"%s\", \"check_value\": \"%s\" }", 
		SUCCESS,
		database_name,
		table_name,
		"Unable to delete Table Row.",
		check_field,
		check_value
		);
            send_response(client_socket, SUCCESS, "application/json", response_body);
        }
	for (int i = 0; i < data_count; i++) {
		free(data_array[i]);
	}
    } else if (strncmp(path, "/delete/table/", 14) == 0 && strcmp(method, "DELETE") == 0) {
	char data[256];
        sscanf(path + 14, "%s", data);
	int data_count = 0;
	char **data_array = split_string(data, "/", &data_count);
	char *database_name = data_array[0];
	char *table_name = data_array[1];
        char response_body[600];
 	int result = deleteTable(database_name, table_name);
        if (result > 0) {
            sprintf(
		response_body, 
		"{ \"status\": \"%s\", \"response\": %d, \"database\": \"%s\", \"table\": \"%s\", \"message\": \"%s\" }", 
		SUCCESS, 
		result, 
		database_name,
		table_name,
		"Table deleted successfully."
		);
            send_response(client_socket, SUCCESS, "application/json", response_body);
        } else {
            sprintf(
		response_body, 
		"{ \"status\": \"%s\", \"response\": null, \"database\": \"%s\", \"table\": \"%s\", \"message\": \"%s\" }", 
		SUCCESS,
		database_name,
		table_name,
		"Unable to delete Table."
		);
            send_response(client_socket, SUCCESS, "application/json", response_body);
        }
	for (int i = 0; i < data_count; i++) {
		free(data_array[i]);
	}
    } else {
        send_response(
	    client_socket, 
	    NOT_FOUND, 
	    "application/json", "{ \"status\": \"404 Not Found\", \"response\": null, \"message\": \"Path not found.\" }");
    }
}
