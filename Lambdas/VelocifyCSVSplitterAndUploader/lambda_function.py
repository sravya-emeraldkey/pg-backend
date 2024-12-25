import boto3
import csv
import io

s3 = boto3.client('s3')

def split_csv_and_upload(bucket_name, input_file_key, rows_per_file=2000):
    try:
        # Step 1: Fetch the main CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=input_file_key)
        csv_content = response['Body'].read().decode('utf-8')
        
        # Step 2: Parse the CSV file
        reader = csv.reader(io.StringIO(csv_content))
        header = next(reader)  # Read the header row
        
        # Step 3: Generate the base name for the output files
        base_name = input_file_key.replace('.csv', '')  # Remove .csv from the file name
        
        # Step 4: Split and upload the CSV files
        file_counter = 1
        rows = []
        
        for row in reader:
            rows.append(row)
            # Check if we've reached the desired number of rows
            if len(rows) >= rows_per_file:
                # Create a new chunk file and upload it to S3
                upload_chunk_to_s3(bucket_name, base_name, file_counter, header, rows)
                file_counter += 1
                rows = []  # Reset rows
        
        # Upload any remaining rows as the last chunk
        if rows:
            upload_chunk_to_s3(bucket_name, base_name, file_counter, header, rows)
        
        print(f"Successfully split and uploaded CSV files to bucket '{bucket_name}'.")
        return {
            "statusCode": 200,
            "body": f"Split and uploaded {file_counter} CSV files to bucket '{bucket_name}'."
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }

def upload_chunk_to_s3(bucket_name, base_name, chunk_number, header, rows):
    # Create a new CSV content for this chunk
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)  # Write header
    writer.writerows(rows)  # Write rows
    chunk_file_content = output.getvalue()
    
    # Generate the S3 key for this chunk
    chunk_file_key = f"{base_name}-part{chunk_number}.csv"
    
    # Upload the chunk to S3
    s3.put_object(Bucket=bucket_name, Key=chunk_file_key, Body=chunk_file_content)
    print(f"Uploaded: {chunk_file_key}")

def lambda_handler(event, context):
    bucket_name = "velocify-calls"  # S3 bucket name
    input_file_key = "Lm32481_CallHistory_20241119_205746_6b80c371-a30a-4933-a83e-b57a5d38c303.csv"  # Name of the main CSV file in the bucket
    
    return split_csv_and_upload(bucket_name, input_file_key)
