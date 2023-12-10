#include<bits/stdc++.h>
using namespace std;

const double K = 1.5; // Parameter for adjusting CUSUM
const int WINDOW_SIZE = 5; // Window size for moving average

double calculateEuclideanDistance(const vector<int>& dataPoint1, const vector<double>& dataPoint2) {
  double distance = 0.0;
  for (int i = 0; i < dataPoint1.size(); ++i) {
    distance += pow(dataPoint1[i] - dataPoint2[i], 2);
  }
  return sqrt(distance);
}

double calculateMovingAverage(const deque<double>& window) {
  double sum = accumulate(window.begin(), window.end(), 0.0);
  return sum / window.size();
}

double calculateMedianAbsoluteDeviation(const deque<double>& window, double median) {
  deque<double> deviations;
  for (double value : window) {
    deviations.push_back(abs(value - median));
  }
  sort(deviations.begin(), deviations.end());
  return deviations[deviations.size() / 2];
}

int main() {
  ifstream inputFile("generator.txt");
  if (!inputFile.is_open()) {
    cerr << "Error: Could not open file!" << endl;
    return 1;
  }

  double sumUp = 0.0; // Cumulative sum for upward direction
  double sumDown = 0.0; // Cumulative sum for downward direction
  int lineNumber = 1; // Track line number
  deque<double> window; // Window for moving average

  // Read data from file
  string line;
  while (getline(inputFile, line)) {
    vector<int> dataPoint;

    // Parse data point from line
    stringstream ss(line);
    int item;
    while (ss >> item) {
      dataPoint.push_back(item);
    }

    // Calculate the distance from the expected value
    double distance = calculateEuclideanDistance(dataPoint, {0.0});
    window.push_back(distance);
    if (window.size() > WINDOW_SIZE) {
      window.pop_front();
    }
    double expectedDistance = calculateMovingAverage(window);

    // Calculate the residual
    double median = window[window.size() / 2];
    double residual = calculateMedianAbsoluteDeviation(window, median);

    // Update cumulative sums
    sumUp = max(0.0, sumUp + residual - K);
    sumDown = min(0.0, sumDown + residual + K);

    // Check for drift based on fixed thresholds
    if (sumUp > 0.0) {
      cout << "Concept drift detected at line number " << lineNumber << "! Upward CUSUM: " << sumUp << endl;
      sumUp = 0; // Reset CUSUM value
    } else if (sumDown < 0.0) {
      cout << "Concept drift detected at line number " << lineNumber << "! Downward CUSUM: " << sumDown << endl;
      sumDown = 0; // Reset CUSUM value
    }

    lineNumber++;
  }

  inputFile.close();

  return 0;
}