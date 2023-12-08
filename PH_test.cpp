#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <sstream>

// Page Hinkley test parameters
const double ALPHA = 0.01;            // Adjust based on your data
const double THRESHOLD = 1;         // Adjust based on your data
const int MIN_NUM_INSTANCES = 10;      // Adjust based on your data

// Function to calculate the Page Hinkley test statistic
double calculatePageHinkley(const std::map<std::string, int>& itemCounter) {
    double phk = 0.0;
    double sum = 0.0;

    for (const auto& entry : itemCounter) {
        sum += entry.second - ALPHA;

        if (sum < 0) {
            sum = 0;
        }

        if (sum > phk) {
            phk = sum;
        }
    }

    return phk;
}

int main() {
    std::ifstream inputFile("generator.txt");
    std::string line;
    std::map<std::string, int> items;
    std::map<std::string, int> itemCounter;

    for (int i = 1; i <= 10; i++) {
        std::string item = "item" + std::to_string(i);
        items[item] = i;
    }

    if (inputFile.is_open()) {
        while (std::getline(inputFile, line)) {
            // Parse the line and extract the 5 items
            // Assuming the items are space-separated
            std::string item;
            std::stringstream ss(line);
            while (ss >> item) {
                // Increment the counter for each item
                itemCounter[item]++;
            }

            // Calculate the Page Hinkley test statistic
            double phk = calculatePageHinkley(itemCounter);
            std::cout << "\nPage Hinkley statistic: " << phk << std::endl;

            // Output the item counter for debugging
            std::cout << "Item counter: ";
            for (const auto& entry : itemCounter) {
                std::cout << entry.first << "=" << entry.second << " ";
            }
            std::cout << std::endl;

            // Check if there is a concept drift
            if (phk > THRESHOLD) {
                std::cout << "Concept drift detected! PHK value: " << phk << std::endl;
            }

            // Clear the item counter for the next line
            itemCounter.clear();
        }

        inputFile.close();
    } else {
        std::cout << "Failed to open input file!" << std::endl;
    }

    return 0;
}
