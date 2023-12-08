#include <bits/stdc++.h>
using namespace std;

class ADWIN
{
private:
    struct Bucket
    {
        int size;
        double sum;
    };

    vector<Bucket> buckets;
    int bucketSize;
    double delta;
    int totalSize;
    double lastChange;
    double driftThreshold;

public:
    ADWIN(double delta, int bucketSize, double driftThreshold) : delta(delta), bucketSize(bucketSize), driftThreshold(driftThreshold)
    {
        buckets.push_back({0, 0.0});
        totalSize = 0;
        lastChange = 0.0;
    }

    void processFile(const string &filePath)
    {
        ifstream file(filePath);
        string line;
        int line_c = 0;
        while (getline(file, line))
        {
            istringstream iss(line);
            string number;
            vector<double> values;

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
            updateBuckets(mean);

            double globalMean = calculateMean();
            double variance = calculateVariance(globalMean);

            if(detectDrift(variance))
            {
                cout << "Drift detected at line: " << line_c << endl; 
            }

            totalSize++;
            line_c++;
        }

        file.close();
    }

private:
    double calculateMean()
    {
        double sum = 0.0;
        int count = 0;

        for (const auto &bucket : buckets)
        {
            sum += bucket.sum;
            count += bucket.size;
        }

        return sum / count;
    }

    double calculateVariance(double mean)
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

    bool detectDrift(double variance)
    {
        if (totalSize <= bucketSize)
        {
            return false;
        }

        double p = 1.0 / totalSize;
        double q = 1.0 / (totalSize - bucketSize);
        double z = sqrt(2 * log(1 / delta) * p * q);

        // double driftThreshold = z * sqrt(variance / bucketSize) + 2 * log(1 / delta) / bucketSize;

        if (variance - lastChange > driftThreshold)
        {
            lastChange = variance;
            buckets.clear();
            buckets.push_back({0, 0.0});
            totalSize = 0;
            lastChange = 0.0;
            return true;
        }

        return false;
    }

    void updateBuckets(double mean)
    {
        if (buckets.size() > bucketSize)
        {
            buckets.erase(buckets.begin());
        }

        buckets.push_back({1, mean});
    }
};

int main()
{
    ADWIN adwin(0.1, 10, 0.1);
    adwin.processFile("/home/yash/Desktop/Clg/7th_Sem/SDS/Project/generator.txt");

    return 0;
}