import json
import pandas as pd
from vnstock import financial_flow
from datetime import datetime
import boto3
import io

def lambda_handler(event, context):
    tickers = [
        'CKV', 'CMG', 'CMT', 'ELC', 'FPT', 'HIG', 'HPT', 'ICT', 'ITD', 'KST',
        'LTC', 'ONE', 'PMJ', 'PMT', 'POT', 'SAM', 'SBD', 'SGT', 'SMT', 'SRA',
        'SRB', 'ST8', 'TST', 'UNI', 'VEC', 'VIE', 'VLA', 'VTC', 'VTE'
    ]

    result = income_statement_to_s3(tickers)

    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }

def income_statement_to_s3(tickers):
    s3_client = boto3.client('s3')
    bucket_name = 'dat-lt'
    folder_prefix = 'raw/income_statement'
    results = []

    for ticker in tickers:
        try:
            print(f'Processing {ticker}')
            df = financial_flow(symbol=ticker, report_type='incomestatement', report_range='yearly')
            df.reset_index(inplace=True)
            df = df.fillna(0).infer_objects()
            df['year'] = df['index']
            df.drop(columns='index', inplace=True)
            df['ticker'] = ticker

            timestamp = datetime.now().strftime('%Y%m%d')
            file_key = f"{folder_prefix}/income_statement_{ticker}_{timestamp}.json"

            json_buffer = io.BytesIO()
            json_data = df.to_json(orient='records', force_ascii=False)
            json_buffer.write(json_data.encode('utf-8'))
            json_buffer.seek(0)

            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_key,
                Body=json_buffer.getvalue(),
                ContentType='application/json'
            )

            results.append({
                'ticker': ticker,
                'status': 'success',
                'file_key': file_key,
                'records_count': len(df)
            })
        except Exception as ex:
            print(f'Error processing {ticker}: {ex}')
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
