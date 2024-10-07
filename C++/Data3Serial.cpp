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

string toLower(const string &str) {
    string lowerStr = str;
    transform(lowerStr.begin(), lowerStr.end(), lowerStr.begin(), ::tolower);
    return lowerStr;
}

int main(int argc, char *argv[]) {
    auto start = chrono::high_resolution_clock::now();
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
    vector<map<string, string>> data;

    if (getline(file, line)) {
        headers = split(line, ',');
    }

    // Read the rest of the file to store the data in a vector of maps
    while (getline(file, line)) {
        vector<string> row = split(line, ',');
        map<string, string> rowMap;
        for (size_t i = 0; i < headers.size(); ++i) {
            if (i < row.size()) {
                rowMap[headers[i]] = row[i];
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
            // Print the matching row if headers size match with row
            if(rowMap.size() == headers.size()){
                for (const auto &header : headers) {
                    cout << header << ": " << rowMap.at(header) << " | ";
                }
            }
            //Else return entire row 
            else{
                cout << "Row with mismatched size:" << endl;
                for (const auto &pair : rowMap) {
                    cout << pair.first << ": " << pair.second << " | ";
                }
            }
            cout << endl;
            auto end = chrono::high_resolution_clock::now();
            chrono::duration<double> executionTime = end - start;
            cout << "Time spent: " << executionTime.count() << " seconds" << endl;
            break;
        }
    }

    if (!found) {
        cout << "No match found for " << headerKey << " = " << headerValue << endl;
    }

    return 0;
}
