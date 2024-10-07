#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <string>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <omp.h>

using namespace std;
using recursive_directory_iterator = std::filesystem::recursive_directory_iterator;

std::mutex output_mutex; // Mutex for thread-safe output

// Function to split a string by a delimiter
vector<string> split(const string &s, char delimiter) {
    vector<string> tokens;
    string token;
    istringstream tokenStream(s);
    while (getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

bool hasCSVExtension(const string& filename) {
    return filename.substr(filename.size() - 4) == ".csv";
}

void processCSVFile(const string& filePath, const string& headerKey, const string& headerValue, const vector<string>& headers) {
    ifstream file(filePath);
    if (!file.is_open()) {
        cerr << "Error: File " << filePath << " could not be opened" << endl;
        return;
    }

    string line;
    vector<map<string, string>> data;

    //map each cell based on headers in rowMap
    while (getline(file, line)) {
        vector<string> row = split(line, ',');
        map<string, string> rowMap;
        for (size_t i = 0; i < headers.size(); i++) {
            if (i < row.size()) {
                rowMap[headers[i]] = row[i];
            }
        }
        data.push_back(rowMap);
    }
    file.close();

    //loop across each rowMap item and return if match is found
    for (const auto &rowMap : data) {
        if (rowMap.find(headerKey) != rowMap.end() && rowMap.at(headerKey) == headerValue) {
            // Lock the output for safe access from multiple threads
            std::lock_guard<std::mutex> lock(output_mutex);
            
            // Print the matching row
            if(rowMap.size() == headers.size()){
                for (const auto &header : headers) {
                    cout << header << ": " << rowMap.at(header) << " | ";
                }
            } else {
                cout << "Row with mismatched size:" << endl;
                for (const auto &pair : rowMap) {
                    cout << pair.first << ": " << pair.second << " | ";
                }
            }
            cout << endl;
            break;
        }
    }
}

// Main function to search through files in parallel using OpenMP
int main(int argc, char *argv[]) {
    auto start = chrono::high_resolution_clock::now();
    std::string directoryPath = "../Data Sets/Data2 - AirNow 2020 California Complex Fire";

    if (argc < 3) {
        cout << "No search keyword entered" << endl;
        return 1;
    }

    string headerKey = argv[1];
    string headerValue = argv[2];
    //custom headers
    vector<string> headers = {"lat", "lon", "time", "measurement_ozone", "measurement_PM2.5", "measurement_PM10", "measurement_CO", "measurement_NO2", "measurement_SO2", "location1", "location2", "data1", "data2"};

    // recursively store all csv files
    vector<string> filePaths;
    for (const auto& entry : recursive_directory_iterator(directoryPath)) {
        filePaths.push_back(entry.path().string());
    }

    // Parallel processing of files using OpenMP
    #pragma omp parallel for
    for (int i = 0; i < filePaths.size(); ++i) {
        if (hasCSVExtension(filePaths[i])) {
            processCSVFile(filePaths[i], headerKey, headerValue, headers);
        }
    }

    // End the main program
    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> totalExecutionTime = end - start;
    cout << "Total Time spent: " << totalExecutionTime.count() << " seconds" << endl;

    return 0;
}
