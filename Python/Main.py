import json
from flask import Flask, request, jsonify
import os
import csv
import time
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import subprocess

app = Flask(__name__)

def parallel_search_in_single_csv(file_path, header_row, search_header, search_term, chunk_size=1000, max_workers=4):
    results = []
    chunk_iterator = pd.read_csv(file_path, header=header_row - 1, chunksize=chunk_size)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(search_in_csv_chunk, chunk, search_header, search_term)
            for chunk in chunk_iterator
        ]
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                results.append(f"Error occurred: {exc}")

    return results

def search_in_csv_chunk(chunk, search_header, search_term):
    matching_rows = chunk[chunk[search_header].astype(str).str.contains(search_term, case=False, na=False)]
    result = matching_rows.to_dict(orient='records')
    return result

def get_all_csv_files_from_root(file_path):
    all_csv_files = []
    for foldername, subfolders, filenames in os.walk(file_path):
            for filename in filenames:
                if filename.endswith('.csv'):
                    all_csv_files.append(os.path.join(foldername, filename).replace('\\','/'))
    return all_csv_files

def chunkify(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def process_chunk(files_chunk, header_row, search_header, search_term, isMultipleFiles = False):
    results = []
    for file_path in files_chunk:
        result = search_in_csv(file_path, header_row, search_header, search_term, isMultipleFiles)
        results.extend(result)
    return results

def parallel_search(file_path, header_row, search_header, search_term, isMultipleFiles=False, chunk_size=1000):
    if isMultipleFiles:
        all_csv_files = get_all_csv_files_from_root(file_path)
        file_chunks = chunkify(all_csv_files, chunk_size)
        with ThreadPoolExecutor() as executor:
            results = executor.map(lambda chunk: process_chunk(chunk, header_row, search_header, search_term, isMultipleFiles = True), file_chunks)
        return list(results)
    else:
        final_results = parallel_search_in_single_csv(file_path, header_row, search_header, search_term, chunk_size=1000, max_workers=4)
        return final_results

def search_in_csv(file_path, header_row, search_header, search_term, isCustomHeader=False):
    if not os.path.exists(file_path):
        return f"File {file_path} not found", 404
    try:
        if isCustomHeader:
            custom_headers = [
                'lat', 'lon', 'time', 'measurement_ozone', 'measurement_PM2.5',
                'measurement_PM10', 'measurement_CO', 'measurement_NO2',
                'measurement_SO2', 'location1', 'location2', 'data1', 'data2'
            ]
            df = pd.read_csv(file_path, header=None, skiprows=header_row - 1)
            df.columns = custom_headers
        else:
            df = pd.read_csv(file_path, header=header_row - 1)
        
        if search_header not in df.columns:
            return {"error": "Invalid search header"}, 400
        
        matching_rows = df[df[search_header].astype(str).str.contains(search_term, case=False, na=False)]        
        result = matching_rows.to_dict(orient='records')        
        return result
    
    except Exception as e:
        return str(e)


@app.route('/pythonData1', methods=['GET'])
def data1_search():
    try:
        start_time = time.time()
        data = request.args
        algorithm = data.get('algorithm')
        file_path = '../Data Sets/Data1 - World Bank Population Data/API_SP.POP.TOTL_DS2_en_csv_v2_3401680.csv'
        header_row = 5
        search_header = data.get('search_header', '')   
        search_term = data.get('search_term', '')

        if algorithm == 'serial':
            results = [search_in_csv(file_path, header_row, search_header, search_term)]
        elif algorithm == 'parallel':
            results = parallel_search(file_path, header_row, search_header, search_term)
        else:
            return jsonify({"error": "Invalid algorithm. Choose 'serial' or 'parallel'."}), 400
        return jsonify({"result": results, "Time taken is": time.time() - start_time})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/pythonData2', methods=['GET'])
def data2_search():
    try:
        start_time = time.time()
        data = request.args
        algorithm = data.get('algorithm')
        file_path = '../Data Sets/Data2 - AirNow 2020 California Complex Fire'
        header_row = 0
        search_header = data.get('search_header', '')
        search_term = data.get('search_term', '')

        if algorithm == 'serial':
            all_csv_files = get_all_csv_files_from_root(file_path)
            for file in all_csv_files:
                results = [search_in_csv(file, header_row, search_header, search_term, isCustomHeader = True)]
        elif algorithm == 'parallel':
            results = parallel_search(file_path, header_row, search_header, search_term, isMultipleFiles = True)
        else:
            return jsonify({"error": "Invalid algorithm. Choose 'serial' or 'parallel'."}), 400
        return jsonify({"result": results, "Time taken is": time.time() - start_time})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/pythonData3', methods=['GET'])
def data3_search():
    try:
        start_time = time.time()
        data = request.args
        algorithm = data.get('algorithm')
        file_path = '../Data Sets/Data3 - NYC Data Organization/Parking_Violations_Issued_-_Fiscal_Year_2022.csv'
        header_row = 1
        search_header = data.get('search_header', '')
        search_term = data.get('search_term', '')

        if algorithm == 'serial':
            results = [search_in_csv(file_path, header_row, search_header, search_term)]
        elif algorithm == 'parallel':
            results = parallel_search(file_path, header_row, search_header, search_term)
            results = [result for result in results if result]
        else:
            return jsonify({"error": "Invalid algorithm. Choose 'serial' or 'parallel'."}), 400
        return jsonify({"result": results, "Time taken is": time.time() - start_time})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/cppData1', methods=['GET'])
def run_cpp1():
    try:
        data = request.args
        algorithm = data.get('algorithm', 'serial')
        search_header = data.get('search_header', '')
        search_term = data.get('search_term', '')
        # Call the C++ executable using subprocess
        command = ['../C++/Data1Serial', search_header, search_term] if algorithm == 'serial' else ['../C++/Data1Parallel', search_header, search_term]
        result = subprocess.run(command, capture_output=True, text=True)
        output = result.stdout
        return jsonify({"message": output})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/cppData2', methods=['GET'])
def run_cpp2():
    try:
        data = request.args
        algorithm = data.get('algorithm', 'serial')
        search_header = data.get('search_header', '')
        search_term = data.get('search_term', '')
        command = ['../C++/Data2Serial', search_header, search_term] if algorithm == 'serial' else ['../C++/Data2Parallel', search_header, search_term]
        result = subprocess.run(command, capture_output=True, text=True)
        return jsonify({"message": result.stdout})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/cppData3', methods=['GET'])
def run_cpp3():
    try:
        data = request.args
        algorithm = data.get('algorithm', 'serial')
        search_header = data.get('search_header', '')
        search_term = data.get('search_term', '')
        command = ['../C++/Data3Serial', search_header, search_term] if algorithm == 'serial' else ['../C++/Data3Parallel', search_header, search_term]
        result = subprocess.run(command, capture_output=True, text=True)
        return jsonify({"message": result.stdout})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

#Flask server
if __name__ == '__main__':
    app.run(debug=True)
    