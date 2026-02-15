import json
import requests
from typing import List, Tuple, Any
from timeit import default_timer as timer

import modules.logging as logging
from modules.logging import logLevel as logLevel
import modules.shared as shared


def generate_create_table(
    p_jobID:int,
    p_description: List[Tuple[Any, ...]],
    p_tablename: str,
    p_source_db_name: str,
    p_target_db_name: str,
) -> str:
    """
    Generate a CREATE TABLE statement for a target database based on column metadata
    from a source database using an LLM via OpenAI-compatible API.

    Args:
        p_description: The cursor.description object from source database
        p_source_db_name: Name of source database (e.g., 'mysql', 'postgresql', 'oracle')
        p_target_db_name: Name of target database (e.g., 'mysql', 'postgresql', 'oracle')
        api_endpoint: OpenAI-compatible API endpoint URL
        api_key: Optional API key for authentication

    Returns:
        String containing the generated CREATE TABLE statement
    """

    logging.logPrint(f'called, create statement for [{p_target_db_name}] target, from [{p_source_db_name}] source')

    tStart = timer()

    # Convert p_description to structured JSON format
    columns_info = []
    for col in p_description:

        # Handle non-serializable types in Oracle's DbType
        type_code = col[1]
        if hasattr(type_code, 'name'):
            # For Oracle's DbType objects, use the name attribute
            type_name = type_code.name
        elif hasattr(type_code, '__name__'):
            # For other objects with __name__ attribute
            type_name = type_code.__name__
        else:
            # For simple types or when name attribute is not available
            type_name = str(type_code)

        column_info = {
            'name': col[0],
            'type_code': type_name,
            'display_size': col[2],
            'internal_size': col[3],
            'precision': col[4],
            'scale': col[5],
            'null_ok': col[6]
        }
        columns_info.append(column_info)

    # Prepare the prompt for the LLM
    prompt = f"""
You are an expert database administrator who specializes in database schema migration.
Your task is to generate a CREATE TABLE statement for a {p_target_db_name} database based on the column metadata from a {p_source_db_name} database.

The column metadata is provided in JSON format below:
{json.dumps(columns_info, indent=2)}

Instructions:
1. Analyze the column metadata and determine appropriate data types for {p_target_db_name}
2. Consider that different databases have different data type mappings
3. Generate a valid CREATE TABLE statement for {p_target_db_name} database
4. Handle NULL/NOT NULL constraints appropriately
5. Keep the column order as provided in the metadata
6. Add appropriate constraints where applicable (primary keys, etc.)
7. the table is to be named as {p_tablename}
8. prefix the statement with the correct drop if exists statement

The DROP TABLE and CREATE TABLE statements should be in valid SQL syntax for {p_target_db_name}.

Return ONLY the SQL statement without any additional text, comments, or explanations.
"""

    # Prepare the request payload
    payload = {
        "model": f"{shared.ai_api_model}",
        "messages": [
            {"role": "system", "content": "You are a helpful database expert."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 1000
    }

    logging.logPrint(f'LLM call payload:[\n{json.dumps(payload, indent=2)}\n]',logLevel.DEBUG)

    # Set up headers
    headers = {
        "Content-Type": "application/json"
    }

    if shared.ai_api_key:
        headers["Authorization"] = f"Bearer {shared.ai_api_key}"

    try:
        # Send request to the LLM API
        response = requests.post(
            shared.ai_api_endpoint,
            headers=headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()

        # Extract the SQL from the response
        result = response.json()
        create_statement = result['choices'][0]['message']['content'].strip()

        tEnd = timer()

        logging.logPrint(f'returned statement:[{create_statement}]',logLevel.DEBUG)

        logging.statsPrint('aiGeneratedCreateStatement', p_jobID, 0, tEnd - tStart, 0 )

        return create_statement

    except Exception as e:
        tEnd = timer()
        logging.statsPrint('aiGeneratedCreateStatementError', p_jobID, 0, tEnd - tStart, 0 )
        logging.processError(p_e=e, p_message=f"Failed to generate CREATE TABLE statement", p_stop=True)
        return ''
