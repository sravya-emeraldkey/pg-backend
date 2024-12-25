# PG-BACKEND

**Priority Gold Backend Repository**

This repository contains Lambda functions and utilities for backend processing in the Priority Gold project. The primary focus is on integrating and managing data from Velocify,Callrail and RingCentral platforms, handling S3 bucket operations, and pushing data to AWS Redshift.

---

## **Project Structure**

### **Directories**

- **Lambdas/**: Contains AWS Lambda functions for various data processing tasks.
  - **deleteVelocifyOldCallLogsFromS3**: Deletes old call logs from S3.
  - **deleteVelocifyOldCallRecordingsFromS3**: Deletes old call recordings from S3.
  - **deleteVelocifyOldLeadsFromS3**: Deletes old lead records from S3.
  - **exportJsonsToCSV**: Exports JSON data to CSV format.
  - **processCallRailData**: Processes call data from CallRail.
  - **processRingCentralData**: Processes data from RingCentral.
  - **processVelocifyData**: Processes data from Velocify.
  - **pushBrokersToRedshift**: Pushes broker data to Redshift.
  - **pushVelocifyCallLogsToRedshift**: Pushes Velocify call logs to Redshift.
  - **pushVelocifyLeadsToRedshift**: Pushes Velocify lead data to Redshift.
  - **redshift-utility**: Contains utility scripts for Redshift.
  - **ringCentralRecordingDownloader**: Downloads recordings from RingCentral.
  - **VelocifyCSVSplitterAndUploader**: Splits large Velocify CSV files and uploads them to S3.

- **Layers/**: Contains shared dependencies packaged as Lambda layers.
  - **jwt.zip**: JWT library for authentication.
  - **requests.zip**: Requests library for HTTP operations.

---

## **Setup Instructions**

### Prerequisites
1. AWS CLI configured with appropriate permissions.
2. Python 3.9 or later.
3. AWS SDKs (boto3).

### Installation
1. Clone this repository.
   ```bash
   git clone <repository-url>
   cd pg-backend
   ```
2. Install necessary dependencies for the Lambda functions (if any).
3. Deploy the Lambdas using your CI/CD pipeline or manually using AWS CLI.

### Deployment
1. Package Lambda functions with their dependencies.
2. Deploy them to your AWS account using the AWS Management Console or AWS CLI.

---

## **Usage**

### Key Features
- **Data Management**: Handles data flow between Velocify, RingCentral, and Redshift.
- **S3 Operations**: Processes and manages files in S3 buckets.
- **Redshift Integration**: Supports SQL operations to insert, update, and delete data in Redshift.
- **Data Cleanup**: Includes scripts to remove old or unnecessary data from S3.

### Workflow
- Velocify , Callrail and Ringcentral data is processed and validated.
- Processed data is uploaded to S3.
- Data is pushed into Redshift using the respective Lambda functions.

---

## **Contributing**

1. Fork the repository.
2. Create a new branch.
3. Commit changes and push to your branch.
4. Create a Pull Request.

---

## **License**
This project is licensed under the MIT License.

---

## **Contact**
For questions or support, reach out to the backend development team.

---

## **Acknowledgments**
- **Priority Gold Team** for the project initiative.
- Open-source contributors for dependencies used in the project.
