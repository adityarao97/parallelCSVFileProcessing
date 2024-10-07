#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <string>
#include <algorithm>
#include <mutex>
#include <chrono>
#include <atomic>
#include <omp.h>
#include <thread>

using namespace std;

mutex mtx;
auto start = chrono::high_resolution_clock::now();
atomic<bool> matchFound(false);

vector<string> split(const string &s, char delimiter) {
    vector<string> tokens;
    string token;
    istringstream tokenStream(s);
    while (getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

void processChunk(const vector<string>& headers, const vector<string>& lines, const string& headerKey, const string& headerValue) {
    for (const string& line : lines) {
        //check if match is found by any other thread return true
        if (matchFound.load()) return;

        vector<string> row = split(line, ',');
        map<string, string> rowMap;

        //map each element with header key in rowMap
        for (size_t i = 0; i < headers.size(); ++i) {
            if (i < row.size()) {
                rowMap[headers[i]] = row[i];
            }
        }

        //if provided headerKey has headerValue in rowMap return it
        if (rowMap.find(headerKey) != rowMap.end() && rowMap.at(headerKey) == headerValue) {
            #pragma omp critical
            {
                if (!matchFound.load()) {
                    //if header size is same as defined in headers map it and then return
                    if (rowMap.size() == headers.size()) {
                        for (const auto &header : headers) {
                            cout << header << ": " << rowMap.at(header) << " | ";
                        }
                    }
                    //return the data as is
                    else {
                        cout << "Row with mismatched size: " << endl;
                        for (const auto &pair : rowMap) {
                            cout << pair.first << ": " << pair.second << " | ";
                        }
                    }
                    auto end = chrono::high_resolution_clock::now();
                    chrono::duration<double> executionTime = end - start;
                    cout << "\nTime spent: " << executionTime.count() << " seconds" << endl;
                    matchFound.store(true);
                }
            }
            return;
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        cout << "No search keyword entered" << endl;
        return 1;
    }

    string headerKey = argv[1];
    string headerValue = argv[2];

    ifstream file("../Data Sets/Data3 - NYC Data Organization/Parking_Violations_Issued_-_Fiscal_Year_2022.csv");
    if (!file.is_open()) {
        cerr << "Error: Could not open the file." << endl;
        return 1;
    }

    string line;
    vector<string> headers;
    vector<string> lines;

    // Read 1st line as headers
    if (getline(file, line)) {
        headers = split(line, ',');
    }

    //store all the remaining content in lines
    while (getline(file, line)) {
        lines.push_back(line);
    }

    file.close();

    //tried with std::thread::hardware_concurrency() to allocate dynamic threads but manual number of threads resulted in improved latency
    const size_t numThreads = 12;
    size_t chunkSize = lines.size() / numThreads;

    #pragma omp parallel for num_threads(numThreads)
    for (size_t i = 0; i < numThreads; ++i) {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? lines.size() : (i + 1) * chunkSize;
        vector<string> chunk(lines.begin() + start, lines.begin() + end);
        processChunk(headers, chunk, headerKey, headerValue);
    }

    if (!matchFound.load()) {
        cout << "No match found for " << headerKey << " = " << headerValue << endl;
    }

    return 0;
}