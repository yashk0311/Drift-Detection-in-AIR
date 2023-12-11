#include<bits/stdc++.h>
using namespace std;

const double K = 1.5; 
const int WINDOW_SIZE = 5;

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

  double sumUp = 0.0; 
  double sumDown = 0.0; 
  int lineNumber = 1; 
  deque<double> window;

  // Read data from file
  string line;
  while (getline(inputFile, line)) {
    vector<int> dataPoint;

    stringstream ss(line);
    int item;
    while (ss >> item) {
      dataPoint.push_back(item);
    }

    double distance = calculateEuclideanDistance(dataPoint, {0.0});
    window.push_back(distance);
    if (window.size() > WINDOW_SIZE) {
      window.pop_front();
    }
    double expectedDistance = calculateMovingAverage(window);

    double median = window[window.size() / 2];
    double residual = calculateMedianAbsoluteDeviation(window, median);

    sumUp = max(0.0, sumUp + residual - K);
    sumDown = min(0.0, sumDown + residual + K);

    if (sumUp > 0.0) {
      cout << "Concept drift detected at line number " << lineNumber << "! Upward CUSUM: " << sumUp << endl;
      sumUp = 0; 
    } else if (sumDown < 0.0) {
      cout << "Concept drift detected at line number " << lineNumber << "! Downward CUSUM: " << sumDown << endl;
      sumDown = 0; 
    }

    lineNumber++;
  }

  inputFile.close();

  return 0;
}