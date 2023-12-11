#include <bits/stdc++.h>
using namespace std;

// DDM parameters
const double LAMBDA = 0.2;
const double THRESHOLD = 0.5; 
const int WINDOW_SIZE = 5; 


double calculateDistance(const vector<string>& dataPoint1, const vector<string>& dataPoint2) {
    int differences = 0;

    for (int i = 0; i < dataPoint1.size(); i++) {
        if (dataPoint1[i] != dataPoint2[i]) {
            differences++;
        }
    }

    return (double)differences / dataPoint1.size();
}

double calculateStandardDeviation(const vector<double>& distances) {
    double mean = 0.0;
    for (const double& distance : distances) {
        mean += distance;
    }
    mean /= distances.size();

    double variance = 0.0;
    for (const double& distance : distances) {
        variance += pow(distance - mean, 2);
    }
    variance /= distances.size() - 1;

    return sqrt(variance);
}

int main() {
    ifstream inputFile("generator.txt");
    string line;
    int lineNumber = 1;
    vector<string> currentLineItems;
    vector<vector<string>> referenceWindow(WINDOW_SIZE);
    vector<double> distances; 
    vector<double> meanDistances; 
    vector<double> standardDeviations; 

    int i = 0;
    while (i < WINDOW_SIZE && getline(inputFile, line)) {
        currentLineItems.clear();
        stringstream ss(line);
        string item;
        while (ss >> item) {
            currentLineItems.push_back(item);
        }

        referenceWindow[i] = currentLineItems;
        ++i;
    }

    if (i != WINDOW_SIZE) {
        cout << "Failed to read enough data points!" << endl;
        return 1;
    }

    for (int j = 0; j < WINDOW_SIZE; j++) {
        distances.push_back(calculateDistance(currentLineItems, referenceWindow[j]));
    }
    meanDistances.push_back(accumulate(distances.begin(), distances.end(), 0.0) / WINDOW_SIZE);
    standardDeviations.push_back(calculateStandardDeviation(distances));
    distances.clear();

    while (getline(inputFile, line)) {
        ++lineNumber;
        currentLineItems.clear();
        stringstream ss(line);
        string item;
        while (ss >> item) {
            currentLineItems.push_back(item);
        }

        
        referenceWindow.erase(referenceWindow.begin());
        referenceWindow.push_back(currentLineItems);
        distances.clear();
        
        for (const auto& dataPoint : referenceWindow) {
            distances.push_back(calculateDistance(currentLineItems, dataPoint));
        }

        meanDistances.push_back(accumulate(distances.begin(), distances.end(), 0.0) / WINDOW_SIZE);
        standardDeviations.push_back(calculateStandardDeviation(distances));

        // Check for drift using z-score
        double zScore = abs(meanDistances.back() - meanDistances.front()) / (standardDeviations.front() * sqrt(2));

        if (zScore > THRESHOLD) {
            cout << "Concept drift detected at line number " << lineNumber << "! z-Score: " << zScore << endl;
        }

        distances.clear();
    }

    inputFile.close();

    return 0;
}
