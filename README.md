# PG-BACKEND

## Overview
Priority Gold Backend (PG-Backend) repository contains serverless functions and configurations to manage backend operations for Priority Gold. The system integrates AWS services like Lambda, EventBridge, S3, Redshift, and more to handle workflows, data processing, and ETL tasks efficiently.

---

## Project Structure

```
PG-BACKEND/
├── EventBridge/            # EventBridge rule configurations for triggering workflows
├── IAM/                    # IAM roles and policies for access management
├── Lambdas/                # AWS Lambda functions for processing various tasks
│   ├── config/             # Configuration folder
│   │   ├── .env            # Environment variables for Lambdas
│   │   ├── permissions.py  # Defines permission settings
│   │   ├── layers.py       # Configuration of Lambda layers
│   ├── deleteVelocifyOldCallLogsFromS3/
│   ├── deleteVelocifyOldCallRecordingsFromS3/
│   ├── deleteVelocifyOldLeadsFromS3/
│   ├── exportJsonsToCSV/
│   ├── processCallRailData/
│   ├── processRingCentralData/
│   ├── processVelocifyData/
│   ├── pushBrokersToRedshift/
│   ├── pushVelocifyCallLogsToRedshift/
│   ├── pushVelocifyLeadsToRedshift/
│   ├── redshift-utility/
│   ├── ringCentralRecordingDownloader/
│   ├── VelocifyCVSSplitterAndUploader/
├── Layers/                 # Shared Lambda layers (e.g., dependencies like JWT, Requests)
├── Redshift/               # Redshift configurations and scripts
│   ├── pg-redshift-cluster/ # Redshift cluster setup and configurations
├── S3/                     # S3 bucket structure and data handling configurations
├── SecretManager/          # AWS Secrets Manager for storing sensitive data
├── README.md               # Documentation for the repository
```

---

## Added Services

### EventBridge
Manages EventBridge rules for triggering Lambda functions based on specified schedules or events.

### IAM
Contains JSON files for defining roles and policies, ensuring secure access to AWS services.

### Config Directory (Inside Lambdas)
- **.env**: Contains environment variables required by Lambda functions.
- **permissions.py**: Configures Lambda permissions and roles for accessing AWS resources.
- **layers.py**: Manages and configures shared Lambda layers for dependencies.

### Service-Specific Configurations
Each service folder includes a `details.py` file with explanations and main configurations for that particular service.

---

## How to Use

### Prerequisites
- **AWS CLI** configured with access to your account.
- Python installed.
- Proper IAM roles and permissions set up.

### Steps
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd PG-BACKEND
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Deploy a Lambda function:
   ```bash
   cd Lambdas/<function-folder>
   zip -r function.zip .
   aws lambda update-function-code --function-name <lambda-name> --zip-file fileb://function.zip
   ```

4. Update `.env` with appropriate environment variables.

---

## Contributing
1. Fork the repository.
2. Create a new branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Commit your changes:
   ```bash
   git commit -m "Add some feature"
   ```
4. Push the branch:
   ```bash
   git push origin feature/your-feature-name
   ```
5. Open a pull request.

---

## License
This repository is licensed under the [MIT License](LICENSE).
