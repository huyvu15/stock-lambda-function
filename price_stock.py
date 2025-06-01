import json
import pandas as pd
from vnstock import stock_historical_data
from datetime import datetime
import boto3
import io

def lambda_handler(event, context):
    # Danh sách các mã cổ phiếu
    tickers = [
        'CKV', 'CMG', 'CMT', 'ELC', 'FPT', 'HIG', 'HPT', 'ICT',
        'ITD', 'KST', 'LTC', 'ONE', 'PMJ', 'PMT', 'POT', 'SAM', 'SBD',
        'SGT', 'SMT', 'SRA', 'SRB', 'ST8', 'TST', 'UNI', 'VEC', 'VIE',
        'VLA', 'VTC', 'VTE'
    ]
    
    result = priceStock(tickers)
    
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }

def priceStock(tickers):
    s3_client = boto3.client('s3')
    bucket_name = 'dat-lt'
    folder_prefix = 'raw/price_stock'
    
    results = []

    for ticker in tickers:
        try:
            print(f'Processing {ticker}')
            
            # Lấy dữ liệu từ 2010 đến hôm nay
            start_date = '2010-01-01'
            end_date = str(datetime.now().date())

            df = stock_historical_data(ticker, start_date, end_date)
            df = df.fillna(0).infer_objects()
            df['Ticker'] = ticker

            # Tên file S3 (không chia theo folder mã cổ phiếu)
            timestamp = datetime.now().strftime('%Y%m%d')
            file_key = f"{folder_prefix}/price_stock_{ticker}_{timestamp}.json"

            # Ghi buffer JSON
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
