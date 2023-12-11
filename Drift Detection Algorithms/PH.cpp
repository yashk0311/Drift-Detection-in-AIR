#include <bits/stdc++.h>
using namespace std;

class PageHinkley {
private:
    double sum;
    double min_sum;
    double threshold;

public:
    PageHinkley(double threshold) : sum(0.0), min_sum(0.0), threshold(threshold) {}

    bool update(double value, double mean) {
        sum += value - mean;
        min_sum = min(min_sum, sum);
        return (sum - min_sum) > threshold;
    }
};

void processFile(const string &filePath) {
    ifstream file(filePath);
    string line;
    PageHinkley ph(320.0);
    double mean = 0.0;
    int count = 0;
    while (getline(file, line)) {
        istringstream iss(line);
        string id;
        iss >> id;

        vector<double> values;
        string number;
        while (getline(iss, number, ',')) {
            double value = stod(number);
            values.push_back(value);
            mean = (mean * count + value) / (count + 1);
            count++;
        }

        for (double value : values) {
            if (ph.update(value, mean)) {
                cout << "Drift detected at line: " << count << endl;
            }
        }
    }
    file.close();
}

int main() {
    processFile("/home/yash/Desktop/Clg/7th_Sem/SDS/Project/src/lat.csv");
    return 0;
}