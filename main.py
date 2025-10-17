from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
from typing import List, Dict, Optional
import os
import yaml
from functools import lru_cache

# Load configuration from YAML file
def load_config():
    """Load configuration from config.yml"""
    config_path = "config.yml"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

# Load config at startup
config = load_config()

app = FastAPI(
    title="CSV Data API",
    version="1.0.0",
    description="""
    ## CSV Data API
    
    This API provides endpoints to interact with CSV data stored in the system.
    
    ### Features
    
    * **Paginated Data Retrieval** - Fetch CSV data with pagination support
    * **Column Information** - Get metadata about available columns
    * **Dataset Information** - View comprehensive dataset statistics
    * **Search Functionality** - Search for specific values in any column
    
    ### Base URL
    
    All endpoints are relative to the base URL where this API is hosted.
    
    ### Response Format
    
    All successful responses include a `success: true` field along with the requested data.
    """,
)

# Global variable to store the dataframe
df_cache = None

def load_csv_data():
    """Load CSV file into memory once"""
    global df_cache
    if df_cache is None:
        folder = config['csv']['folder']
        filename = config['csv']['filename']
        csv_path = os.path.join(folder, filename)
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at {csv_path}")
        df_cache = pd.read_csv(csv_path)
    return df_cache

@app.on_event("startup")
async def startup_event():
    """Load CSV on startup for better performance"""
    try:
        load_csv_data()
        print(f"✓ CSV file loaded successfully from {config['csv']['folder']}/{config['csv']['filename']}")
    except Exception as e:
        print(f"✗ Error loading CSV: {e}")

@app.get(
    "/",
    tags=["General"],
    summary="API Root",
    response_description="Welcome message and available endpoints"
)
async def root():
    """
    ## Root Endpoint
    
    Returns basic information about the API and available endpoints.
    
    ### Response
    
    Returns a JSON object containing:
    - API welcome message
    - List of available endpoints with descriptions
    """
    return {
        "message": "CSV Data API",
        "endpoints": {
            "/data": "Get paginated data from CSV",
            "/columns": "Get list of columns",
            "/info": "Get dataset information",
            "/search": "Search for specific values in columns"
        }
    }

@app.get(
    "/data",
    tags=["Data"],
    summary="Get Paginated Data",
    response_description="Paginated CSV data with metadata",
    responses={
        200: {
            "description": "Successfully retrieved paginated data",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "page": 1,
                        "page_size": 10,
                        "total_records": 100,
                        "total_pages": 10,
                        "showing_records": 10,
                        "data": [
                            {"column1": "value1", "column2": "value2"},
                            {"column1": "value3", "column2": "value4"}
                        ]
                    }
                }
            }
        },
        404: {
            "description": "Page not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Page 15 not found. Total pages: 10"}
                }
            }
        },
        500: {
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "example": {"detail": "Error reading data: ..."}
                }
            }
        }
    }
)
async def get_data(
    page: int = Query(
        1,
        ge=1,
        description="Page number to retrieve (starts from 1)",
        example=1
    ),
    page_size: int = Query(
        10,
        ge=1,
        le=1000,
        description="Number of records per page (max: 1000)",
        example=10
    )
) -> Dict:
    """
    ## Retrieve Paginated CSV Data
    
    Fetches data from the CSV file with pagination support.
    
    ### Parameters
    
    - **page**: The page number to retrieve (minimum: 1)
    - **page_size**: Number of records per page (minimum: 1, maximum: 1000)
    
    ### Returns
    
    A JSON object containing:
    - **success**: Boolean indicating operation success
    - **page**: Current page number
    - **page_size**: Number of records per page
    - **total_records**: Total number of records in the dataset
    - **total_pages**: Total number of pages available
    - **showing_records**: Number of records in the current response
    - **data**: Array of records (each record is a dictionary with column names as keys)
    
    ### Example Usage
    
    ```
    GET /data?page=1&page_size=20
    ```
    """
    try:
        df = load_csv_data()
        
        # Calculate pagination
        total_records = len(df)
        total_pages = (total_records + page_size - 1) // page_size
        
        if page > total_pages and total_records > 0:
            raise HTTPException(
                status_code=404,
                detail=f"Page {page} not found. Total pages: {total_pages}"
            )
        
        # Calculate start and end indices
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total_records)
        
        # Get paginated data
        paginated_df = df.iloc[start_idx:end_idx]
        
        # Convert to list of dictionaries (column name as key, value row-wise)
        data = paginated_df.to_dict(orient='records')
        
        return {
            "success": True,
            "page": page,
            "page_size": page_size,
            "total_records": total_records,
            "total_pages": total_pages,
            "showing_records": len(data),
            "data": data
        }
    
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

@app.get(
    "/columns",
    tags=["Metadata"],
    summary="Get Column Names",
    response_description="List of all column names in the dataset",
    responses={
        200: {
            "description": "Successfully retrieved column list",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "columns": ["id", "name", "age", "city"],
                        "total_columns": 4
                    }
                }
            }
        }
    }
)
async def get_columns() -> Dict:
    """
    ## Get Column Names
    
    Retrieves a list of all column names available in the CSV dataset.
    
    ### Returns
    
    A JSON object containing:
    - **success**: Boolean indicating operation success
    - **columns**: Array of column names
    - **total_columns**: Total count of columns
    
    ### Example Usage
    
    ```
    GET /columns
    ```
    """
    try:
        df = load_csv_data()
        return {
            "success": True,
            "columns": df.columns.tolist(),
            "total_columns": len(df.columns)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading columns: {str(e)}")

@app.get(
    "/search",
    tags=["Data"],
    summary="Search Data",
    response_description="Filtered and paginated search results",
    responses={
        200: {
            "description": "Successfully retrieved search results",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "page": 1,
                        "page_size": 10,
                        "total_records": 25,
                        "total_pages": 3,
                        "showing_records": 10,
                        "search_column": "name",
                        "search_value": "John",
                        "data": [
                            {"id": 1, "name": "John Doe", "age": 30},
                            {"id": 5, "name": "John Smith", "age": 25}
                        ]
                    }
                }
            }
        },
        400: {
            "description": "Invalid column name",
            "content": {
                "application/json": {
                    "example": {"detail": "Column 'invalid' not found. Available columns: ['id', 'name', 'age']"}
                }
            }
        }
    }
)
async def search_data(
    column: str = Query(
        ...,
        description="Column name to search in",
        example="company_name"
    ),
    value: str = Query(
        ...,
        description="Value to search for (case-insensitive partial match)",
        example="Glencore"
    ),
    page: int = Query(
        1,
        ge=1,
        description="Page number (starts from 1)",
        example=1
    ),
    page_size: int = Query(
        10,
        ge=1,
        le=1000,
        description="Number of records per page (max: 1000)",
        example=10
    )
) -> Dict:
    """
    ## Search for Records
    
    Searches for records where a specified column contains a given value.
    The search is case-insensitive and performs partial matching.
    
    ### Parameters
    
    - **column**: The name of the column to search in (required)
    - **value**: The value to search for (required, case-insensitive)
    - **page**: Page number for pagination (minimum: 1)
    - **page_size**: Number of records per page (minimum: 1, maximum: 1000)
    
    ### Returns
    
    A JSON object containing:
    - **success**: Boolean indicating operation success
    - **page**: Current page number
    - **page_size**: Number of records per page
    - **total_records**: Total number of matching records
    - **total_pages**: Total number of pages for the search results
    - **showing_records**: Number of records in the current response
    - **search_column**: The column that was searched
    - **search_value**: The value that was searched for
    - **data**: Array of matching records
    
    ### Example Usage
    
    ```
    GET /search?column=name&value=John&page=1&page_size=20
    ```
    
    ### Notes
    
    - Search is case-insensitive
    - Performs partial matching (e.g., "John" will match "John Doe", "Johnny", etc.)
    - Returns 400 error if the specified column doesn't exist
    """
    try:
        df = load_csv_data()
        
        if column not in df.columns:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{column}' not found. Available columns: {df.columns.tolist()}"
            )
        
        # Filter data
        filtered_df = df[df[column].astype(str).str.contains(value, case=False, na=False)]
        
        # Pagination
        total_records = len(filtered_df)
        total_pages = (total_records + page_size - 1) // page_size if total_records > 0 else 0
        
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total_records)
        
        paginated_df = filtered_df.iloc[start_idx:end_idx]
        data = paginated_df.to_dict(orient='records')
        
        return {
            "success": True,
            "page": page,
            "page_size": page_size,
            "total_records": total_records,
            "total_pages": total_pages,
            "showing_records": len(data),
            "search_column": column,
            "search_value": value,
            "data": data
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching data: {str(e)}")