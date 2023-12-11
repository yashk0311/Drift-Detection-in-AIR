#include <bits/stdc++.h>
using namespace std;

struct Bucket
{
    int size;
    double sum;
};

vector<Bucket> updateBuckets(vector<Bucket> buckets, double mean, int bucketSize)
{
    if (buckets.size() > bucketSize)
    {
        buckets.erase(buckets.begin());
    }

    buckets.push_back({1, mean});
    return buckets;
}

pair<double, int> calculateMean(vector<Bucket> buckets)
{
    double sum = 0.0;
    int count = 0;

    for (const auto &bucket : buckets)
    {
        sum += bucket.sum;
        count += bucket.size;
    }

    return make_pair(sum / count, count);
}

double calculateVariance(vector<Bucket> buckets, double mean)
{
    double variance = 0.0;
    int count = 0;

    for (const auto &bucket : buckets)
    {
        variance += bucket.size * pow(bucket.sum / bucket.size - mean, 2);
        count += bucket.size;
    }

    return variance / count;
}

pair<bool, vector<Bucket>> detectDrift(double variance, int totalSize, int bucketSize, double lastChange, double delta)
{
    if (totalSize <= bucketSize)
    {
        return make_pair(false, vector<Bucket>());
    }

    double p = 1.0 / totalSize;
    double q = 1.0 / (totalSize - bucketSize);
    double z = sqrt(2 * log(1 / delta) * p * q);

    double driftThreshold = z * sqrt(variance / bucketSize) + 2 * log(1 / delta) / bucketSize;

    if (variance - lastChange > driftThreshold)
    {
        lastChange = variance;
        vector<Bucket> buckets;
        buckets.push_back({0, 0.0});
        totalSize = 0;
        lastChange = 0.0;
        return make_pair(true, buckets);
    }

    return make_pair(false, vector<Bucket>());
}

void processFile(const string &filePath, double delta, int bucketSize, double driftThreshold)
{
    ifstream file(filePath);
    string line;
    int line_c = 0;
    vector<Bucket> buckets;
    buckets.push_back({0, 0.0});
    int totalSize = 0;
    double lastChange = 0.0;
    while (getline(file, line))
    {
        istringstream iss(line);
        string number;
        vector<double> values;
        iss >> number;
        while (iss >> number)
        {
            values.push_back(stod(number));
        }

        if (values.size() != 5)
        {
            cout << "Invalid input: " << line << endl;
            continue;
        }

        double mean = accumulate(values.begin(), values.end(), 0.0) / values.size();
        buckets = updateBuckets(buckets, mean, bucketSize);

        pair<double, int> meanAndCount = calculateMean(buckets);
        double globalMean = meanAndCount.first;
        double variance = calculateVariance(buckets, globalMean);

        pair<bool, vector<Bucket>> driftAndBuckets = detectDrift(variance, totalSize, bucketSize, lastChange, delta);
        if (driftAndBuckets.first)
        {
            cout << "Drift detected at line: " << line_c << endl;
            buckets = driftAndBuckets.second;
        }

        totalSize++;
        line_c++;
    }

    file.close();
}

int main()
{
    processFile("/home/yash/Desktop/Clg/7th_Sem/SDS/Project/src/lat.csv", 0.03, 10, 0.1);

    return 0;
}