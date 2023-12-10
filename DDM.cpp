#include <bits/stdc++.h>
using namespace std;

// DDM parameters
const double LAMBDA = 0.2; // Weight given to recent data points
const double THRESHOLD = 0.5; // Threshold for detecting drift
const int WINDOW_SIZE = 5; // Size of the reference window

// Function to calculate the distance between two data points
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
    int lineNumber = 1; // Track line number
    vector<string> currentLineItems;
    vector<vector<string>> referenceWindow(WINDOW_SIZE); // Store data points in the reference window
    vector<double> distances; // Stores distances from reference window
    vector<double> meanDistances; // Stores average distances over window
    vector<double> standardDeviations; // Stores standard deviations of distances

    // Initialize reference window with first WINDOW_SIZE data points
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

    // Calculate initial statistics
    for (int j = 0; j < WINDOW_SIZE; j++) {
        distances.push_back(calculateDistance(currentLineItems, referenceWindow[j]));
    }
    meanDistances.push_back(accumulate(distances.begin(), distances.end(), 0.0) / WINDOW_SIZE);
    standardDeviations.push_back(calculateStandardDeviation(distances));
    distances.clear();

    // Process remaining data points
    while (getline(inputFile, line)) {
        ++lineNumber; // Update line number
        currentLineItems.clear();
        stringstream ss(line);
        string item;
        while (ss >> item) {
            currentLineItems.push_back(item);
        }

        // Update reference window, calculate distances, and maintain statistics
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
