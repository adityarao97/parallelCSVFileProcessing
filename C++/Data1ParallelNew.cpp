#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <string>
#include <algorithm>
#include <mutex>
#include <chrono>
#include <omp.h>

using namespace std;

mutex mtx;
auto start = chrono::high_resolution_clock::now();

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

// Function to trim leading/trailing whitespace
string trim(const string &str) {
    size_t first = str.find_first_not_of(' ');
    if (first == string::npos) return "";
    size_t last = str.find_last_not_of(' ');
    return str.substr(first, last - first + 1);
}

// Function to replace "population, total" with "population | total"
string replacePopulationTotal(const string &str) {
    string modified = str;
    string target = "population, total";
    string replacement = "population | total";

    size_t pos = modified.find(target);
    if (pos != string::npos) {
        modified.replace(pos, target.length(), replacement);
    }
    return modified;
}

void processChunk(const vector<string>& headers, const vector<string>& lines, const string& headerKey, const string& headerValue) {
    for (const string& line : lines) {
        string modifiedLine = replacePopulationTotal(line);
        vector<string> row = split(modifiedLine, ',');
        map<string, string> rowMap;

        for (size_t i = 0; i < headers.size(); ++i) {
            if (i < row.size()) {
                rowMap[headers[i]] = trim(row[i]);
            }
        }

        // Check if the current row matches the search criteria
        if (rowMap.find(headerKey) != rowMap.end() && rowMap.at(headerKey) == headerValue) {
            lock_guard<mutex> lock(mtx);
            for (const auto &header : headers) {
                cout << header << ": " << rowMap.at(header) << " | ";
            }

            auto end = chrono::high_resolution_clock::now();
            chrono::duration<double> executionTime = end - start;
            cout << "time spent is: " << executionTime.count() << " seconds" << endl;
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

    ifstream file("../Data Sets/Data1 - World Bank Population Data/API_SP.POP.TOTL_DS2_en_csv_v2_3401680.csv");
    if (!file.is_open()) {
        cerr << "Error: Could not open the file." << endl;
        return 1;
    }

    string line;
    vector<string> headers;
    vector<string> lines;

    // Skip the first 4 lines to start reading from the 5th line (which is the header row)
    for (int i = 0; i < 4; ++i) {
        if (!getline(file, line)) {
            cerr << "Error: File has fewer than 5 lines." << endl;
            return 1;
        }
    }

    // Read the 5th line to get the headers
    if (getline(file, line)) {
        headers = split(line, ',');
        for (auto &header : headers) {
            header = trim(header);
        }
    }

    // Read the rest of the file into a vector of strings (each representing a line)
    while (getline(file, line)) {
        lines.push_back(line);
    }

    file.close();

    // Parallel processing using OpenMP
    size_t numThreads = 4;
    size_t chunkSize = lines.size() / numThreads;

    #pragma omp parallel for num_threads(numThreads)
    for (size_t i = 0; i < numThreads; ++i) {
        size_t startIdx = i * chunkSize;
        size_t endIdx = (i == numThreads - 1) ? lines.size() : (i + 1) * chunkSize;
        vector<string> chunk(lines.begin() + startIdx, lines.begin() + endIdx);

        // Process each chunk in parallel
        processChunk(headers, chunk, headerKey, headerValue);
    }

    return 0;
}
