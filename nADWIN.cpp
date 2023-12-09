#include <bits/stdc++.h>
using namespace std;

class ADWIN
{
private:
    double delta;         // Confidence parameter
    double n;             // Number of samples observed so far
    double e_t;           // Current error estimate
    double r_t;           // Current recent count
    double s_t;           // Current non-recent count
    bool change_detected; // Flag for concept change detection
    double window_size;   // Current window size

public:
    ADWIN(double delta, double initial_window_size) : delta(delta), window_size(initial_window_size) {}

    void update(const std::vector<double> &data_point, int line_number)
    {
        n++;

        // Calculate median and rank of the data point
        double median = get_median(data_point);
        int rank = get_rank(data_point, data_point[0]);

        // Calculate error based on rank difference (weighted by window size)
        double error = std::abs(rank - window_size / 2) / window_size;

        // Check for concept drift
        double threshold = r_t / (r_t + s_t) * phi_inv(1 - delta / 2);

        if (error > threshold)
        {
            change_detected = true;
            r_t = 0;       // Reset recent count
            s_t = n - r_t; // Update non-recent count

            // Adjust window size dynamically
            if (s_t > window_size)
            {
                window_size = s_t * 2;
            }

            // Print concept drift message with line number
            std::cout << "Concept drift detected at line: " << line_number << std::endl;
        }
        else
        {
            r_t++;
            s_t = n - r_t;
        }

        // Update error estimate
        e_t = (r_t * error + s_t * e_t) / n;
    }

    bool has_concept_changed() const
    {
        return change_detected;
    }

    double get_error_estimate() const
    {
        return e_t;
    }

private:
    // Function to calculate the median of a vector
    double get_median(const std::vector<double> &data_point) const
    {
        size_t size = data_point.size();
        if (size % 2 == 0)
        {
            return (data_point[size / 2] + data_point[size / 2 - 1]) / 2;
        }
        else
        {
            return data_point[size / 2];
        }
    }

    // Function to calculate the rank of a value in a vector
    int get_rank(const std::vector<double> &data_point, double value) const
    { 
        std::vector<double> sorted_data = data_point;
        std::sort(sorted_data.begin(), sorted_data.end());
        return std::distance(sorted_data.begin(), std::find(sorted_data.begin(), sorted_data.end(), value)) + 1;
    }

    // Inverse function of the cumulative distribution function (CDF) of the standard normal distribution
    double phi_inv(double p) const
    {
        if (p <= 0.0 || p >= 1.0)
        {
            throw std::invalid_argument("Invalid probability value for phi_inv");
        }

        // Use Abramowitz and Stegun approximation
        double a = -39.69683028665379;
        double b = 220.9460984245205;
        double c = -275.9285104488078;
        double d = 138.3577518672690;
        double e = -30.66479806614716;
        double f = 5.772255750784237;

        double z = p < 0.5 ? p : 1 - p;
        double t = sqrt(log(1.0 / (z * z)));
        double x = a + t * (b + t * (c + t * (d + t * (e + t * f))));
        return p < 0.5 ? -x : x;
    }
};

int main()
{
    // Define confidence parameter
    double delta = 0.7;
    double window_size = 5.0;
    // Create ADWIN object
    ADWIN adwin(delta, window_size);

    // Sample data points
    vector<vector<double>> data = {
        {1, 2, 3, 4, 5},
        {1, 2, 3, 4, 5},
        {1, 2, 3, 4, 5},
        {1, 2, 3, 4, 5},
        {1, 2, 3, 4, 5},
        {1, 2, 3, 4, 5},
        {1, 12, 2, 3, 5},
        {1, 12, 2, 3, 5},
        {1, 12, 14, 2, 5},
        {1, 12, 14, 2, 5},
        {1, 14, 15, 2, 5},
        {1, 14, 15, 2, 5},
        {1, 14, 15, 5, 8},
        {1, 14, 15, 5, 8},
        {1, 14, 15, 5, 7},
        {1, 14, 15, 5, 7},
        {1, 10, 15, 5, 7},
        {1, 10, 15, 5, 7},
        {10, 13, 15, 5, 7},
        {10, 13, 15, 5, 7},
        {13, 15, 5, 7, 9},
        {13, 15, 5, 7, 9},
        {13, 15, 6, 7, 9},
        {13, 15, 6, 7, 9},
    };

    // Process each data point and check for concept drift
    for (int i = 0; i < data.size(); i++)
    {
        adwin.update(data[i], i + 1); // + 1 to adjust line numbers starting from 1
    }

    // Print final error estimate
    cout << "Final error estimate: " << adwin.get_error_estimate() << endl;

    return 0;
}