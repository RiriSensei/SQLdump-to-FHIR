This project provides a robust data transformation pipeline that converts healthcare data from a source SQLite database into FHIR (Fast Healthcare Interoperability Resources) standard resources. The pipeline is designed to be modular, scalable, and memory-efficient, utilizing Docker for containerization and supporting large datasets.

An SQL dump namely - task_output.sql had been provided. 
The conversion of this dump to sqlite was done gracefully thanks to a github repo I found
Repo link : https://github.com/ww9/mysql2sqlite

Although the conversion was not perfect, I managed to use the find and replace function in VSCode to remove all the unsupported commands in sqlite like LOCKING and UNLOCKING.



## Architecture

The transformation pipeline consists of two primary stages:

1. **Preprocessing Stage (Python)**: 
   - Extracts data from source SQLite database
   - Cleans and preprocesses data
   - Converts tables to JSON files

2. **FHIR Transformation Stage (Node.js)**: 
   - Reads preprocessed JSON files
   - Maps data to FHIR resource types
   - Stores transformed resources in a new SQLite database

### Supported FHIR Resources
- Patient
- Encounter
- Procedure
- Device

## Prerequisites

- Docker
- Docker Compose
- Source SQLite database 'task.sqlite' (included in the repo)

## Installation

1. Clone the repository

3. Build and run using Docker Compose:

In Terminal, cd to the repository location and run:
docker-compose up --build


## Data Processing Flow

1. **Preprocessing Stage**:
   - Connects to source SQLite database
   - Extracts data from specified tables
   - Performs data cleaning
   - Converts data to JSON format
   - Creates `preprocessing_complete.txt` marker file
   - Python was used for this stage as the pandas library is possibly the best data manipulation tool that exists.

2. **Transformation Stage**:
   - Waits for preprocessing completion
   - Reads JSON files
   - Maps data to FHIR resources using custom mapping logic
   - Saves resources to SQLite database
   - Handles large datasets via streaming

## Customization and Configuration

### Preprocessor Configuration
- Modify `preprocessor.py` to:
  - Add/remove tables
  - Adjust data cleaning logic
  - Change chunk processing size

### Transformer Configuration
- Update `resourceTransform.js` to:
  - Modify FHIR resource mapping
  - Adjust logging levels
  - Add custom transformations

## Logging

- Console logs for real-time monitoring
- Log files in `logs/` directory
  - `transformation.log`: General logs
  - `transformation-error.log`: Error logs

## Performance Optimizations

- Stream-based processing
- Batch processing
- SQLite Write-Ahead Logging (WAL)
- Configurable batch sizes
- Indexed database tables

## Error Handling

- Comprehensive logging
- Graceful error recovery
- Unhandled promise rejection management
