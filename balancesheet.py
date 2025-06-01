import boto3
import pandas as pd
from vnstock import financial_flow
import json
from datetime import datetime
import io

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    # List of stock tickers
    tickers = [
        'CKV', 'CMG', 'CMT', 'ELC', 'FPT', 'HIG', 'HPT', 'ICT', 'ITD', 'KST',
        'LTC', 'ONE', 'PMJ', 'PMT', 'POT', 'SAM', 'SBD', 'SGT', 'SMT', 'SRA',
        'SRB', 'ST8', 'TST', 'UNI', 'VEC', 'VIE', 'VLA', 'VTC', 'VTE'
    ]
    
    result = balanceSheet(tickers)
    
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }

def balanceSheet(tickers):
    # Initialize S3 client
    s3_client = boto3.client('s3')
    bucket_name = 'dat-lt'
    folder_prefix = 'raw/balance_sheet'
    
    results = []
    
    for ticker in tickers:
        try:
            print(f'Processing {ticker}')
            
            # Get financial flow data (balance sheet) quarterly
            new_record = financial_flow(symbol=ticker, report_type='balancesheet', report_range='quarterly')
            new_record.reset_index(inplace=True)
            new_record = new_record.fillna(0)
            
            # Split 'year' and 'quarter' from 'index' column
            new_record[['year', 'quarter']] = new_record['index'].str.split('-Q', expand=True)
            new_record = new_record.drop('index', axis=1)
            
            # Add ticker column for identification
            new_record['ticker'] = ticker
            
            # Generate timestamp for file naming
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Save to S3 as parquet file
            file_key = f"{folder_prefix}/{ticker}/balance_sheet_{ticker}_{timestamp}.parquet"
            
            # Convert DataFrame to parquet buffer
            parquet_buffer = io.BytesIO()
            new_record.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            print(f'Successfully uploaded {ticker} data to S3: {file_key}')
            results.append({
                'ticker': ticker,
                'status': 'success',
                'file_key': file_key,
                'records_count': len(new_record)
            })
            
        except Exception as ex:
            print(f'Error while processing {ticker}: {ex}')
            results.append({
                'ticker': ticker,
                'status': 'error',
                'error': str(ex)
            })
    
    return {
        'processed_tickers': len(tickers),
        'successful': len([r for r in results if r['status'] == 'success']),
        'failed': len([r for r in results if r['status'] == 'error']),
        'details': results
    }
