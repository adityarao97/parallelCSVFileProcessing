#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <string>
#include <algorithm>
#include <chrono>

using namespace std;

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

    // Find the position of "population, total" and replace it
    size_t pos = modified.find(target);
    if (pos != string::npos) {
        modified.replace(pos, target.length(), replacement);
    }
    return modified;
}

int main(int argc, char *argv[]) {
    auto start = chrono::high_resolution_clock::now();
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
    vector<map<string, string>> data;

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

    // Read the rest of the file to store the data in a vector of maps
    while (getline(file, line)) {
        line = replacePopulationTotal(line);
        vector<string> row = split(line, ',');
        map<string, string> rowMap;
        for (size_t i = 0; i < headers.size(); ++i) {
            if (i < row.size()) {
                rowMap[headers[i]] = trim(row[i]);
            }
        }
        data.push_back(rowMap);
    }

    file.close();

    // Search for the row where the header key matches the value
    bool found = false;
    for (const auto &rowMap : data) {
        if (rowMap.find(headerKey) != rowMap.end() && rowMap.at(headerKey) == headerValue) {
            found = true;
            // Print the matching row
            for (const auto &header : headers) {
                cout << header << ": " << rowMap.at(header) << " | ";
            }
            auto end = chrono::high_resolution_clock::now();
            chrono::duration<double> executionTime = end - start;
            cout << "time spent is: " << executionTime.count() << " seconds" <<endl;
        }
    }

    if (!found) {
        cout << "No match found for " << headerKey << " = " << headerValue << endl;
    }

    return 0;
}
